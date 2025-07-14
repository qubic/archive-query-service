package grpc

import (
	"context"
	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/services.mock.go -package=mock -source service.go

var _ api.ArchiveQueryServiceServer = &ArchiveQueryService{}

type TransactionsService interface {
	GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error)
	GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32) ([]*api.Transaction, error)
	GetTransactionsForIdentity(
		ctx context.Context,
		identity string,
		filters map[string]string,
		ranges map[string][]*entities.Range,
		from, size uint32,
	) (*entities.TransactionsResult, error)
}

type TickDataService interface {
	GetTickData(ctx context.Context, tickNumber uint32) (*api.TickData, error)
}

type StatusService interface {
	GetLastProcessedTick(ctx context.Context) (*api.LastProcessedTick, error)
	GetProcessedTickIntervals(ctx context.Context) ([]*api.ProcessedTickInterval, error)
}

type ComputorsListService interface {
	GetComputorsListsForEpoch(ctx context.Context, epoch uint32) ([]*api.ComputorsList, error)
}

type ArchiveQueryService struct {
	srv            *grpc.Server
	grpcListenAddr net.Addr
	api.UnimplementedArchiveQueryServiceServer
	txService     TransactionsService
	tdService     TickDataService
	statusService StatusService
	clService     ComputorsListService
}

func NewArchiveQueryService(txService TransactionsService, tdService TickDataService, statusService StatusService, clService ComputorsListService) *ArchiveQueryService {
	return &ArchiveQueryService{
		txService:     txService,
		tdService:     tdService,
		statusService: statusService,
		clService:     clService,
	}
}

func (s *ArchiveQueryService) GetTransactionByHash(ctx context.Context, req *api.GetTransactionByHashRequest) (*api.GetTransactionByHashResponse, error) {
	tx, err := s.txService.GetTransactionByHash(ctx, req.Hash)
	if err != nil {
		//TODO: Handle specific error cases, e.g., if transaction not found to return NotFound status
		return nil, status.Errorf(codes.Internal, "failed to get transaction by hash: %v", err)
	}

	return &api.GetTransactionByHashResponse{Transaction: tx}, nil
}

func (s *ArchiveQueryService) GetTransactionsForTick(ctx context.Context, req *api.GetTransactionsForTickRequest) (*api.GetTransactionsForTickResponse, error) {
	txs, err := s.txService.GetTransactionsForTickNumber(ctx, req.TickNumber)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get transactions for tick: %v", err)
	}

	return &api.GetTransactionsForTickResponse{Transactions: txs}, nil
}

func (s *ArchiveQueryService) GetTickData(ctx context.Context, req *api.GetTickDataRequest) (*api.GetTickDataResponse, error) {
	td, err := s.tdService.GetTickData(ctx, req.TickNumber)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get tick data: %v", err)
	}

	return &api.GetTickDataResponse{TickData: td}, nil
}

func (s *ArchiveQueryService) GetTransactionsForIdentity(ctx context.Context, request *api.GetTransactionsForIdentityRequest) (*api.GetTransactionsForIdentityResponse, error) {
	err := validateIdentity(request.GetIdentity())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}

	err = validateIdentityTransactionQueryFilters(request.GetFilters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid filter: %v", err)
	}

	ranges, err := validateIdentityTransactionQueryRanges(request.GetFilters(), request.GetRanges())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid range: %v", err)
	}

	from, size, err := validatePagination(request.GetPagination())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid page: %v", err)
	}

	result, err := s.txService.GetTransactionsForIdentity(ctx, request.Identity, request.GetFilters(), ranges, from, size)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get transactions for identity: %v", err)
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

func (s *ArchiveQueryService) GetLastProcessedTick(ctx context.Context, _ *emptypb.Empty) (*api.GetLastProcessedTickResponse, error) {
	lpt, err := s.statusService.GetLastProcessedTick(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get last processed tick: %v", err)
	}

	return &api.GetLastProcessedTickResponse{TickNumber: lpt.TickNumber}, nil
}

func (s *ArchiveQueryService) GetProcessedTickIntervals(ctx context.Context, _ *emptypb.Empty) (*api.GetProcessedTicksIntervalsResponse, error) {
	intervals, err := s.statusService.GetProcessedTickIntervals(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get processed tick intervals: %v", err)
	}

	return &api.GetProcessedTicksIntervalsResponse{ProcessedTicksIntervals: intervals}, nil
}
func (s *ArchiveQueryService) GetComputorsListsForEpoch(ctx context.Context, request *api.GetComputorsListForEpochRequest) (*api.GetComputorsListForEpochResponse, error) {
	computorListsForEpoch, err := s.clService.GetComputorsListsForEpoch(ctx, request.Epoch)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get computors lists: %v", err)
	}

	return &api.GetComputorsListForEpochResponse{
		ComputorsList: computorListsForEpoch,
	}, nil
}
