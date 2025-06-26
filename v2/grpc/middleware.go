package grpc

import (
	"context"
	"fmt"
	"github.com/jellydator/ttlcache/v3"
	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strconv"
	"strings"
	"time"
)

const maxTickCacheKey = "max_tick"
const tickIntervalsCacheKey = "tick_intervals"

type StatusCache struct {
	lastProcessedTickProvider *ttlcache.Cache[string, uint32]
	tickIntervalsProvider     *ttlcache.Cache[string, []*statusPb.TickInterval]
	StatusServiceClient       statusPb.StatusServiceClient
}

func NewStatusCache(statusServiceClient statusPb.StatusServiceClient, ttl time.Duration) *StatusCache {
	lastProcessedTickProvider := ttlcache.New[string, uint32](
		ttlcache.WithTTL[string, uint32](ttl),
		ttlcache.WithDisableTouchOnHit[string, uint32](), // don't refresh ttl upon getting the item from cache
	)

	tickIntervalsProvider := ttlcache.New[string, []*statusPb.TickInterval](
		ttlcache.WithTTL[string, []*statusPb.TickInterval](ttl),
		ttlcache.WithDisableTouchOnHit[string, []*statusPb.TickInterval](), // don't refresh ttl upon getting the item from cache
	)
	return &StatusCache{
		lastProcessedTickProvider: lastProcessedTickProvider,
		tickIntervalsProvider:     tickIntervalsProvider,
		StatusServiceClient:       statusServiceClient,
	}
}

func (s *StatusCache) GetMaxTick(ctx context.Context) (uint32, error) {
	if s.lastProcessedTickProvider.Has(maxTickCacheKey) {
		item := s.lastProcessedTickProvider.Get(maxTickCacheKey)
		if item != nil {
			return item.Value(), nil
		}
	}

	maxTick, err := s.fetchStatusMaxTick(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetching status service max tick: %v", err)
	}

	s.lastProcessedTickProvider.Set(maxTickCacheKey, maxTick, ttlcache.DefaultTTL)
	return maxTick, nil
}

func (s *StatusCache) GetTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	if s.tickIntervalsProvider.Has(tickIntervalsCacheKey) {
		item := s.tickIntervalsProvider.Get(tickIntervalsCacheKey)
		if item != nil {
			return item.Value(), nil
		}
	}

	tickIntervals, err := s.fetchTickIntervals(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching status service max tick: %v", err)
	}

	s.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	return tickIntervals, nil
}

func (s *StatusCache) Start() {
	s.lastProcessedTickProvider.Start()
	s.tickIntervalsProvider.Start()
}

func (s *StatusCache) Stop() {
	s.lastProcessedTickProvider.Stop()
	s.tickIntervalsProvider.Stop()
}

func (s *StatusCache) fetchStatusMaxTick(ctx context.Context) (uint32, error) {
	statusResponse, err := s.StatusServiceClient.GetStatus(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("fetching status service: %v", err)
	}

	return statusResponse.LastProcessedTick, nil
}

func (s *StatusCache) fetchTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	tickIntervalsResponse, err := s.StatusServiceClient.GetTickIntervals(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("fetching tick intervals: %v", err)
	}

	if len(tickIntervalsResponse.Intervals) == 0 {
		return nil, fmt.Errorf("no tick intervals found")
	}

	return tickIntervalsResponse.Intervals, nil
}

type TickWithinBoundsInterceptor struct {
	store       statusPb.StatusServiceClient
	statusCache *StatusCache
}

func NewTickWithinBoundsInterceptor(store statusPb.StatusServiceClient, statusCache *StatusCache) *TickWithinBoundsInterceptor {
	return &TickWithinBoundsInterceptor{store: store, statusCache: statusCache}
}

func (twb *TickWithinBoundsInterceptor) GetInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	var err error

	switch request := req.(type) {
	case *api.GetTickDataRequest:
		err = twb.checkTickWithinArchiverIntervals(ctx, request.TickNumber)
	case *api.GetTransactionsForTickRequest:
		err = twb.checkTickWithinArchiverIntervals(ctx, request.TickNumber)
	case *api.GetTransactionsForIdentityRequest:
		if request.GetFilters() != nil {
			filterVal, ok := request.GetFilters()["tickNumber"]
			if ok {
				var tickNumber uint64
				tickNumber, err = strconv.ParseUint(filterVal, 10, 32)
				if err == nil {
					err = twb.checkTickWithinArchiverIntervals(ctx, uint32(tickNumber))
				}
			}
		}
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
	maxTick, err := twb.statusCache.GetMaxTick(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get max tick from cache: %v", err)
	}

	tickIntervals, err := twb.statusCache.GetTickIntervals(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get tick intervals from cache: %v", err)
	}

	lastProcessedTick := maxTick

	if tickNumber > lastProcessedTick {
		st := status.Newf(codes.FailedPrecondition, "requested tick number %d is greater than last processed tick %d", tickNumber, lastProcessedTick)
		st, err = st.WithDetails(&api.LastProcessedTick{LastProcessedTick: lastProcessedTick})
		if err != nil {
			return status.Errorf(codes.Internal, "creating custom status")
		}
		return st.Err()
	}

	processedTickIntervalsPerEpoch := tickIntervals
	wasSkipped, nextAvailableTick := WasSkippedByArchive(tickNumber, processedTickIntervalsPerEpoch)
	if wasSkipped == true {
		st := status.Newf(codes.OutOfRange, "provided tick number %d was skipped by the system, next available tick is %d", tickNumber, nextAvailableTick)
		st, err = st.WithDetails(&api.NextAvailableTick{NextTickNumber: nextAvailableTick})
		if err != nil {
			return status.Errorf(codes.Internal, "creating custom status")
		}
		return st.Err()
	}

	return nil
}

func WasSkippedByArchive(tick uint32, processedTicksIntervalPerEpoch []*statusPb.TickInterval) (bool, uint32) {
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
