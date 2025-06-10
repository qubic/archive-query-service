package rpc

import (
	"context"
	"fmt"
	"github.com/qubic/archive-query-service/protobuf"
	"github.com/qubic/go-archiver/protobuff"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/qubic/go-node-connector/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strings"
)

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
		return nil, fmt.Errorf("invalid tick number: %w", err)
	}

	h, err := handler(ctx, req)

	return h, err
}

type IdentitiesValidatorInterceptor struct{}

func (i *IdentitiesValidatorInterceptor) GetInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	var err error

	switch request := req.(type) {
	case *protobuf.GetTransactionRequest:
		err = i.checkFormat(request.TxId, true)
	case *protobuf.GetIdentityTransactionsRequest:
		err = i.checkFormat(request.Identity, false)
	case *protobuf.GetTransferTransactionsPerTickRequestV2:
		err = i.checkFormat(request.Identity, false)
	default:
		break
	}

	if err != nil {
		return nil, fmt.Errorf("invalid id format: %w", err)
	}

	h, err := handler(ctx, req)

	return h, err
}

func (i *IdentitiesValidatorInterceptor) checkFormat(idStr string, isLowercase bool) error {
	id := types.Identity(idStr)
	pubKey, err := id.ToPubKey(isLowercase)
	if err != nil {
		return fmt.Errorf("converting id to pubkey: %w", err)
	}

	var pubkeyFixed [32]byte
	copy(pubkeyFixed[:], pubKey[:32])
	id, err = id.FromPubKey(pubkeyFixed, isLowercase)
	if err != nil {
		return fmt.Errorf("converting pubkey back to id: %w", err)
	}

	if id.String() != idStr {
		return fmt.Errorf("original id string %s does not match converted id %s", idStr, id.String())
	}

	return nil
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
		st, err = st.WithDetails(&protobuff.LastProcessedTick{LastProcessedTick: lastProcessedTick})
		if err != nil {
			return status.Errorf(codes.Internal, "creating custom status")
		}
		return st.Err()
	}

	processedTickIntervalsPerEpoch := tickIntervals
	wasSkipped, nextAvailableTick := WasSkippedByArchive(tickNumber, processedTickIntervalsPerEpoch)
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

type LogTechnicalErrorInterceptor struct{}

func (lte *LogTechnicalErrorInterceptor) GetInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	h, err := handler(ctx, req)
	if err != nil {
		statusError, ok := status.FromError(err)
		if ok && statusError.Code() == codes.Internal {
			lastIndex := strings.LastIndex(info.FullMethod, "/")
			var method string
			if lastIndex > 1 && len(info.FullMethod) > lastIndex+1 {
				method = info.FullMethod[lastIndex+1:]
			} else {
				method = info.FullMethod
			}
			log.Printf("[ERROR] [%s] %s: %s", statusError.Code(), method, err.Error())
		}
	}
	return h, err
}
