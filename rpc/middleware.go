package rpc

import (
	"context"
	"github.com/pkg/errors"
	"github.com/qubic/archive-query-service/protobuf"
	"github.com/qubic/go-archiver/protobuff"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TickWithinBoundsInterceptor struct {
	store statusPb.StatusServiceClient
}

func NewTickWithinBoundsInterceptor(store statusPb.StatusServiceClient) *TickWithinBoundsInterceptor {
	return &TickWithinBoundsInterceptor{store: store}
}

func (twb *TickWithinBoundsInterceptor) GetInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	var err error

	switch request := req.(type) {

	case *protobuf.GetTickRequestV2:
		err = twb.checkTickWithinArchiverIntervals(ctx, request.TickNumber)
	case *protobuf.GetTickTransactionsRequest:
		err = twb.checkTickWithinArchiverIntervals(ctx, request.TickNumber)
	case *protobuf.GetTickDataRequest:
		err = twb.checkTickWithinArchiverIntervals(ctx, request.TickNumber)
	case *protobuf.GetTickApprovedTransactionsRequest:
		err = twb.checkTickWithinArchiverIntervals(ctx, request.TickNumber)
	case *protobuf.GetTickTransactionsRequestV2:
		err = twb.checkTickWithinArchiverIntervals(ctx, request.TickNumber)

	default:
		break
	}

	if err != nil {
		return nil, errors.Wrapf(err, "invalid tick number")
	}

	h, err := handler(ctx, req)

	return h, err
}

func (twb *TickWithinBoundsInterceptor) checkTickWithinArchiverIntervals(ctx context.Context, tickNumber uint32) error {

	statusRes, err := twb.store.GetStatus(ctx, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get last processed tick")
	}
	lastProcessedTick := statusRes.LastProcessedTick

	if tickNumber > lastProcessedTick {
		st := status.Newf(codes.FailedPrecondition, "requested tick number %d is greater than last processed tick %d", tickNumber, lastProcessedTick)
		st, err = st.WithDetails(&protobuff.LastProcessedTick{LastProcessedTick: lastProcessedTick})
		if err != nil {
			return status.Errorf(codes.Internal, "creating custom status")
		}
		return st.Err()
	}

	processedTickIntervalsPerEpoch, err := twb.store.GetTickIntervals(ctx, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "getting processed tick intervals per epoch")
	}

	wasSkipped, nextAvailableTick := WasSkippedByArchive(tickNumber, processedTickIntervalsPerEpoch.Intervals)
	if wasSkipped == true {
		st := status.Newf(codes.OutOfRange, "provided tick number %d was skipped by the system, next available tick is %d", tickNumber, nextAvailableTick)
		st, err = st.WithDetails(&protobuff.NextAvailableTick{NextTickNumber: nextAvailableTick})
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
