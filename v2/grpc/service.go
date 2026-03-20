package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/grpc/filters"
	"github.com/qubic/archive-query-service/v2/grpc/utils"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/services.mock.go -package=mock -source service.go

var _ api.ArchiveQueryServiceServer = &ArchiveQueryService{}

type TransactionsService interface {
	GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error)
	GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32, filters map[string][]string, ranges map[string][]entities.Range) ([]*api.Transaction, error)
	GetTransactionsForIdentity(
		ctx context.Context,
		identity string,
		queryFilters entities.Filters,
		from, size uint32,
	) (*entities.TransactionsResult, error)
}

type TickDataService interface {
	GetTickData(ctx context.Context, tickNumber uint32) (*api.TickData, error)
}

type StatusService interface {
	GetStatus(ctx context.Context) (*statusPb.GetStatusResponse, error)
	GetProcessedTickIntervals(ctx context.Context) ([]*api.ProcessedTickInterval, error)
}

type ComputorsListService interface {
	GetComputorsListsForEpoch(ctx context.Context, epoch uint32) ([]*api.ComputorList, error)
}

type EventsService interface {
	GetEvents(ctx context.Context, queryFilters entities.Filters, from, size uint32) (*entities.EventsResult, error)
}

type ArchiveQueryService struct {
	srv            *grpc.Server
	grpcListenAddr net.Addr
	api.UnimplementedArchiveQueryServiceServer
	txService      TransactionsService
	tdService      TickDataService
	statusService  StatusService
	clService      ComputorsListService
	evService      EventsService
	pageSizeLimits PageSizeLimits
}

func NewArchiveQueryService(
	txService TransactionsService, tdService TickDataService, statusService StatusService,
	clService ComputorsListService, evService EventsService, pageSizeLimits PageSizeLimits,
) *ArchiveQueryService {
	return &ArchiveQueryService{
		txService:      txService,
		tdService:      tdService,
		statusService:  statusService,
		clService:      clService,
		evService:      evService,
		pageSizeLimits: pageSizeLimits,
	}
}

func (s *ArchiveQueryService) GetTransactionByHash(ctx context.Context, req *api.GetTransactionByHashRequest) (*api.GetTransactionByHashResponse, error) {
	tx, err := s.txService.GetTransactionByHash(ctx, req.Hash)
	if err != nil {
		return nil, createInternalError(fmt.Sprintf("failed to get transaction by hash [%v]", req.GetHash()), err)
	}
	if tx == nil {
		return nil, status.Error(codes.NotFound, "transaction not found")
	}
	return &api.GetTransactionByHashResponse{Transaction: tx}, nil
}

func (s *ArchiveQueryService) GetTransactionsForTick(ctx context.Context, req *api.GetTransactionsForTickRequest) (*api.GetTransactionsForTickResponse, error) {
	filterMap, err := filters.CreateTickTransactionsFilters(req.GetFilters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid filters: %v", err)
	}

	ranges, err := filters.ValidateTickTransactionQueryRanges(filterMap, req.GetRanges())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid range: %v", err)
	}

	txs, err := s.txService.GetTransactionsForTickNumber(ctx, req.TickNumber, filterMap, ranges)
	if err != nil {
		return nil, createInternalError(fmt.Sprintf("failed to get transactions for tick [%d]", req.GetTickNumber()), err)
	}

	return &api.GetTransactionsForTickResponse{Transactions: txs}, nil
}

func (s *ArchiveQueryService) GetTickData(ctx context.Context, req *api.GetTickDataRequest) (*api.GetTickDataResponse, error) {
	// it is important that the tick range is checked in advance because a nil result will be returned as an empty tick and not as 404
	td, err := s.tdService.GetTickData(ctx, req.TickNumber)
	if err != nil {
		return nil, createInternalError(fmt.Sprintf("failed to get tick data for tick [%d]", req.GetTickNumber()), err)
	}

	return &api.GetTickDataResponse{TickData: td}, nil
}

func (s *ArchiveQueryService) GetTransactionsForIdentity(ctx context.Context, request *api.GetTransactionsForIdentityRequest) (*api.GetTransactionsForIdentityResponse, error) {
	err := utils.ValidateIdentity(request.GetIdentity())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}

	// we need to stay backwards compatible here. exclude filters are postfixed with -exclude.
	includes, excludes := filters.SplitDeprecatedIncludeExcludeFilters(request.GetFilters())
	if len(excludes) > 0 && len(request.GetExclude()) > 0 { // old and new api mismatch
		return nil, status.Errorf(codes.InvalidArgument, "cannot use both -exclude filters postfix and exclude filters together")
	} else if len(excludes) == 0 { // use new exclude filters
		excludes = request.GetExclude()
	}

	includeFilters, err := filters.CreateIdentityTransactionFilters(includes)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "creating include filters: %v", err)
	}

	excludeFilters, err := filters.CreateIdentityTransactionFilters(excludes)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "creating exclude filters: %v", err)
	}

	err = filters.ValidateExcludeFilterKeys(excludeFilters)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	filterRanges, err := filters.CreateIdentityTransactionQueryRanges(request.GetRanges())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid range: %v", err)
	}

	queryFilters := entities.Filters{Include: includeFilters, Exclude: excludeFilters, Ranges: filterRanges}
	err = filters.VerifyNoConflictingFilters(queryFilters)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "conflicting filters: %v", err)
	}

	from, size, err := s.pageSizeLimits.ValidatePagination(request.GetPagination())
	if err != nil {
		// debug log temporarily. we need to find out how many users use strange pagination parameters.
		log.Printf("[DEBUG] Invalid pagination: %v. Request: %v", err, request)
		return nil, status.Errorf(codes.InvalidArgument, "invalid pagination: %v", err)
	}

	result, err := s.txService.GetTransactionsForIdentity(ctx, request.Identity, queryFilters, from, size)
	if err != nil {
		return nil, createInternalError(fmt.Sprintf("failed to get transactions for identity [%s]", request.GetIdentity()), err)
	}

	// paging information
	apiHits := &api.Hits{
		Total: uint32(result.GetHits().GetTotal()), //nolint: gosec
		From:  from,
		Size:  size,
	}

	return &api.GetTransactionsForIdentityResponse{
		ValidForTick: result.LastProcessedTick,
		Hits:         apiHits,
		Transactions: result.GetTransactions(),
	}, nil
}

func createInternalError(message string, err error) error {
	log.Printf("[ERROR] %s: %v", message, err)
	return status.Error(codes.Internal, message)
}

func (s *ArchiveQueryService) GetLastProcessedTick(ctx context.Context, _ *emptypb.Empty) (*api.GetLastProcessedTickResponse, error) {
	cachedStatus, err := s.statusService.GetStatus(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get status: %v", err)
	}

	return &api.GetLastProcessedTickResponse{
		TickNumber:              cachedStatus.LastProcessedTick,
		Epoch:                   cachedStatus.ProcessingEpoch,
		IntervalInitialTick:     cachedStatus.IntervalInitialTick,
		LogLastProcessedTick: cachedStatus.EventsLastProcessedTick,
	}, nil
}

func (s *ArchiveQueryService) GetProcessedTickIntervals(ctx context.Context, _ *emptypb.Empty) (*api.GetProcessedTickIntervalsResponse, error) {
	intervals, err := s.statusService.GetProcessedTickIntervals(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get processed tick intervals: %v", err)
	}

	return &api.GetProcessedTickIntervalsResponse{ProcessedTickIntervals: intervals}, nil
}

func (s *ArchiveQueryService) GetComputorsListsForEpoch(ctx context.Context, request *api.GetComputorListsForEpochRequest) (*api.GetComputorListsForEpochResponse, error) {
	computorListsForEpoch, err := s.clService.GetComputorsListsForEpoch(ctx, request.Epoch)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get computors lists: %v", err)
	}

	if len(computorListsForEpoch) == 0 {
		return nil, status.Error(codes.NotFound, "computor lists not found")
	}

	return &api.GetComputorListsForEpochResponse{
		ComputorsLists: computorListsForEpoch,
	}, nil
}

func (s *ArchiveQueryService) GetEvents(ctx context.Context, req *api.GetEventsRequest) (*api.GetEventsResponse, error) {
	includeFilters, err := filters.CreateEventFilters(req.GetFilters(), filters.AllowedEventIncludeFilters)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "creating include filters: %v", err)
	}

	excludeFilters, err := filters.CreateEventFilters(req.GetExclude(), filters.AllowedEventExcludeFilters)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "creating exclude filters: %v", err)
	}

	queryRanges, err := filters.CreateEventRanges(req.GetRanges(), filters.AllowedEventRanges)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "creating range filters: %v", err)
	}

	shouldFilters, err := filters.CreateShouldFilters(req.GetShould(), filters.AllowedEventShouldFilters, filters.AllowedEventShouldRanges)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "creating should filters: %v", err)
	}

	queryFilters := entities.Filters{Include: includeFilters, Exclude: excludeFilters, Ranges: queryRanges, Should: shouldFilters}
	err = filters.VerifyNoConflictingFilters(queryFilters)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "conflicting filters: %v", err)
	}

	from, size, err := s.pageSizeLimits.ValidatePagination(req.GetPagination())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid pagination: %v", err)
	}

	cachedStatus, err := s.statusService.GetStatus(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get status: %v", err)
	}
	eventsLastProcessedTick := cachedStatus.GetEventsLastProcessedTick()

	if tickValues, ok := includeFilters[filters.EventFilterTickNumber]; ok && len(tickValues) > 0 {
		tickNumber, convErr := strconv.ParseUint(tickValues[0], 10, 32)
		if convErr == nil && uint32(tickNumber) > eventsLastProcessedTick {
			st := status.Newf(codes.FailedPrecondition, "requested tick number %d is greater than last processed tick %d", tickNumber, eventsLastProcessedTick)
			st, detailErr := st.WithDetails(&api.LastProcessedTick{TickNumber: eventsLastProcessedTick})
			if detailErr != nil {
				return nil, status.Errorf(codes.Internal, "creating custom status")
			}
			return nil, st.Err()
		}
	}

	result, err := s.evService.GetEvents(ctx, queryFilters, from, size)
	if err != nil {
		return nil, createInternalError("failed to get events", err)
	}

	apiHits := &api.Hits{
		Total: uint32(result.GetHits().GetTotal()), //nolint: gosec
		From:  from,
		Size:  size,
	}

	return &api.GetEventsResponse{
		ValidForTick: eventsLastProcessedTick,
		Hits:         apiHits,
		Events:       result.GetEvents(),
	}, nil
}

func (s *ArchiveQueryService) GetHealth(context.Context, *emptypb.Empty) (*api.HealthResponse, error) {
	return &api.HealthResponse{
		Status: "UP",
	}, nil
}
