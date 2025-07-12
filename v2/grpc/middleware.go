package grpc

import (
	"context"
	"fmt"
	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strings"
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
	maxTick, err := twb.statusService.GetLastProcessedTick(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get max tick from cache: %v", err)
	}

	tickIntervals, err := twb.statusService.GetProcessedTickIntervals(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get tick intervals from cache: %v", err)
	}

	lastProcessedTick := maxTick.TickNumber

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
