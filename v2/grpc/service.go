package grpc

import (
	"context"
	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ api.ArchiveQueryServiceServer = &ArchiveQueryService{}

type TransactionsService interface {
	GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error)
	GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32) ([]*api.Transaction, error)
	GetTransactionsForIdentity(ctx context.Context, identity string, filters *api.GetTransactionsForIdentityFilters, aggregations *api.GetTransactionsForIdentityAggregations, page *api.Page) ([]*api.Transaction, error)
}

type TickDataService interface {
	GetTickData(ctx context.Context, tickNumber uint32) (*api.TickData, error)
}

type ArchiveQueryService struct {
	api.UnimplementedArchiveQueryServiceServer
	txService TransactionsService
	tdService TickDataService
}

func NewArchiveQueryService(txService TransactionsService, tdService TickDataService) *ArchiveQueryService {
	return &ArchiveQueryService{
		txService: txService,
		tdService: tdService,
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
	//TODO: Implement pagination and sorting logic request
	err := validateIdentity(request.GetIdentity())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	err = validateTransactionFilters(request.GetFilters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid filters: %v", err)
	}
	err = validateTransactionAggregations(request.GetFilters(), request.GetAggregations())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid aggregations: %v", err)
	}
	err = validatePage(request.GetPage())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid page parameter: %v", err)
	}
	txs, err := s.txService.GetTransactionsForIdentity(ctx, request.Identity, request.GetFilters(), request.GetAggregations(), request.GetPage())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get transactions for identity: %v", err)
	}

	return &api.GetTransactionsForIdentityResponse{Transactions: txs}, nil
}
