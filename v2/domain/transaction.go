package domain

import (
	"context"
	"errors"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/transactions.mock.go -package=mock -source transaction.go
type TransactionRepository interface {
	GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error)
	GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32, filters map[string][]string, ranges map[string][]*entities.Range) ([]*api.Transaction, error)
	GetTransactionsForIdentity(
		ctx context.Context,
		identity string,
		maxTick uint32,
		filters map[string][]string,
		ranges map[string][]*entities.Range,
		from, size uint32,
	) ([]*api.Transaction, *entities.Hits, error)
}

type StatusFetcherFunc func(ctx context.Context) (*statusPb.GetStatusResponse, error)

type TransactionService struct {
	statusFetcher StatusFetcherFunc
	repo          TransactionRepository
}

func NewTransactionService(repo TransactionRepository, statusFetcher StatusFetcherFunc) *TransactionService {
	return &TransactionService{
		statusFetcher: statusFetcher,
		repo:          repo,
	}
}

func (s *TransactionService) GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error) {
	tx, err := s.repo.GetTransactionByHash(ctx, hash)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	return tx, err
}

func (s *TransactionService) GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32, filters map[string][]string, ranges map[string][]*entities.Range) ([]*api.Transaction, error) {
	return s.repo.GetTransactionsForTickNumber(ctx, tickNumber, filters, ranges)
}

func (s *TransactionService) GetTransactionsForIdentity(ctx context.Context, identity string, filters map[string][]string,
	ranges map[string][]*entities.Range, from, size uint32) (*entities.TransactionsResult, error) {

	status, err := s.statusFetcher(ctx)
	if err != nil || status == nil || status.LastProcessedTick < 1 {
		return nil, err
	}
	txs, hits, err := s.repo.GetTransactionsForIdentity(ctx, identity, status.LastProcessedTick, filters, ranges, from, size)
	return &entities.TransactionsResult{LastProcessedTick: status.LastProcessedTick, Hits: hits, Transactions: txs}, err

}
