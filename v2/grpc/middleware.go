package grpc

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type TickWithinBoundsInterceptor struct {
	statusService StatusService
}

func NewTickWithinBoundsInterceptor(statusService StatusService) *TickWithinBoundsInterceptor {
	return &TickWithinBoundsInterceptor{statusService: statusService}
}

func (twb *TickWithinBoundsInterceptor) GetInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	var err error

	switch request := req.(type) {

	case *api.GetTickDataRequest:
		err = twb.checkTickWithinArchiverIntervals(ctx, request.TickNumber)
	case *api.GetTransactionsForTickRequest:
		err = twb.checkTickWithinArchiverIntervals(ctx, request.TickNumber)

	default:
		break
	}

	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Errorf("invalid tick number: %w", err).Error())
	}

	h, err := handler(ctx, req)

	return h, err
}

type IdentitiesValidatorInterceptor struct{}

func (i *IdentitiesValidatorInterceptor) GetInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	var err error

	switch request := req.(type) {
	case *api.GetTransactionByHashRequest:
		err = i.checkFormat(request.Hash, true)
	case *api.GetTransactionsForIdentityRequest:
		err = i.checkFormat(request.Identity, false)
	default:
		break
	}

	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Errorf("invalid id format: %w", err).Error())
	}

	h, err := handler(ctx, req)

	return h, err
}

func (i *IdentitiesValidatorInterceptor) checkFormat(idStr string, isLowercase bool) error {
	return validateDigest(idStr, isLowercase)
}

func (twb *TickWithinBoundsInterceptor) checkTickWithinArchiverIntervals(ctx context.Context, tickNumber uint32) error {
	cachedStatus, err := twb.statusService.GetStatus(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get status from cache: %v", err)
	}

	tickIntervals, err := twb.statusService.GetProcessedTickIntervals(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get tick intervals from cache: %v", err)
	}

	lastProcessedTick := cachedStatus.LastProcessedTick

	if tickNumber > lastProcessedTick {
		st := status.Newf(codes.FailedPrecondition, "requested tick number %d is greater than last processed tick %d", tickNumber, lastProcessedTick)
		st, err = st.WithDetails(&api.LastProcessedTick{TickNumber: lastProcessedTick})
		if err != nil {
			return status.Errorf(codes.Internal, "creating custom status")
		}
		return st.Err()
	}

	processedTickIntervalsPerEpoch := tickIntervals
	wasSkipped, nextAvailableTick := WasSkippedByArchive(tickNumber, processedTickIntervalsPerEpoch)
	if wasSkipped {
		st := status.Newf(codes.OutOfRange, "provided tick number %d was skipped by the system, next available tick is %d", tickNumber, nextAvailableTick)
		st, err = st.WithDetails(&api.NextAvailableTick{NextTickNumber: nextAvailableTick})
		if err != nil {
			return status.Errorf(codes.Internal, "creating custom status")
		}
		return st.Err()
	}

	return nil
}

func WasSkippedByArchive(tick uint32, processedTicksIntervalPerEpoch []*api.ProcessedTickInterval) (bool, uint32) {
	if len(processedTicksIntervalPerEpoch) == 0 {
		return false, 0
	}

	for _, interval := range processedTicksIntervalPerEpoch {
		if tick < interval.FirstTick {
			return true, interval.FirstTick
		}
		if tick >= interval.FirstTick && tick <= interval.LastTick {
			return false, 0
		}
	}

	return false, 0
}

type LogTechnicalErrorInterceptor struct{}

func (lte *LogTechnicalErrorInterceptor) GetInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	h, err := handler(ctx, req)
	if err != nil {
		statusError, _ := status.FromError(err)
		if statusError.Code() == codes.Internal || statusError.Code() == codes.Unknown {
			lastIndex := strings.LastIndex(info.FullMethod, "/")
			var method string
			if lastIndex > 1 && len(info.FullMethod) > lastIndex+1 {
				method = info.FullMethod[lastIndex+1:]
			} else {
				method = info.FullMethod
			}
			log.Printf("[ERROR] [%s] %s: %s. Request: %v", statusError.Code(), method, err.Error(), req)
		}
	}
	return h, err
}

type Cacheable interface {
	GetCacheKey() string
	GetTTL() time.Duration
}

type RedisCacheInterceptor struct {
	redisClient *redis.Client
}

func NewRedisCacheInterceptor(redisClient *redis.Client) *RedisCacheInterceptor {
	return &RedisCacheInterceptor{redisClient: redisClient}
}

func (rci *RedisCacheInterceptor) GetInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// we first need to check if the request is cacheable, if not, we just call the handler
	t, ok := req.(Cacheable)
	if !ok {
		return handler(ctx, req)
	}

	// then we need to get the cache key which is defined by the request itself
	// normally a combination of the method name and request parameters
	key := t.GetCacheKey()

	// if response found in cache, return it
	cachedResponse, err := getCachedResponse(ctx, rci.redisClient, key, req)
	if err == nil {
		return cachedResponse, nil
	}

	// otherwise call the handler to get the response
	response, err := handler(ctx, req)
	if err != nil {
		return response, err
	}

	// then proceed to cache the response and even if caching fails for multiple reasons like redis cluster unavailable
	// we still return the response
	err = cacheResponse(ctx, rci.redisClient, key, response, t.GetTTL())
	if err != nil {
		log.Printf("failed to cache response: %v", err)
	}

	return response, nil
}

func getCachedResponse(ctx context.Context, redisClient *redis.Client, key string, response any) (proto.Message, error) {
	b, err := redisClient.Get(ctx, key).Bytes()
	if err != nil {
		return nil, fmt.Errorf("getting cached response from redis: %w", err)
	}

	var res anypb.Any
	err = proto.Unmarshal(b, &res)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling cached response into anypb.Any: %w", err)
	}

	msg, err := res.UnmarshalNew()
	if err != nil {
		return nil, fmt.Errorf("unmarshalling anypb.Any into proto.Message: %w", err)
	}

	return msg, nil
}

func cacheResponse(ctx context.Context, redisClient *redis.Client, key string, response any, ttl time.Duration) error {
	msg, _ := response.(proto.Message)
	anyRes, err := anypb.New(msg)
	if err != nil {
		return fmt.Errorf("calling anypb.New: %w", err)
	}

	b, err := proto.Marshal(anyRes)
	if err != nil {
		return fmt.Errorf("marshalling anyRes: %w", err)
	}
	err = redisClient.Set(ctx, key, b, ttl).Err()
	if err != nil {
		return fmt.Errorf("setting redis key: %w", err)
	}

	return nil
}
