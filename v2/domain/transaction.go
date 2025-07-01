package domain

import (
	"context"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/transactions.mock.go -package=mock -source transaction.go
type TransactionRepository interface {
	GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error)
	GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32) ([]*api.Transaction, error)
	GetTransactionsForIdentity(ctx context.Context, identity string, maxTick uint32, filters map[string]string, ranges map[string][]*entities.Range, from, size uint32) ([]*api.Transaction, *entities.Hits, error)
}

type MaxTickFetcherFunc func(ctx context.Context) (uint32, error)

type TransactionService struct {
	maxTickFetcher MaxTickFetcherFunc
	repo           TransactionRepository
}

func NewTransactionService(repo TransactionRepository, maxTickFetcher MaxTickFetcherFunc) *TransactionService {
	return &TransactionService{
		maxTickFetcher: maxTickFetcher,
		repo:           repo,
	}
}

func (s *TransactionService) GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error) {
	return s.repo.GetTransactionByHash(ctx, hash)
}

func (s *TransactionService) GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32) ([]*api.Transaction, error) {
	return s.repo.GetTransactionsForTickNumber(ctx, tickNumber)
}

func (s *TransactionService) GetTransactionsForIdentity(ctx context.Context, identity string, filters map[string]string, ranges map[string][]*entities.Range, from, size uint32) (*entities.TransactionsResult, error) {
	maxTick, err := s.maxTickFetcher(ctx)
	if err != nil || maxTick < 1 {
		return nil, err
	}
	txs, hits, err := s.repo.GetTransactionsForIdentity(ctx, identity, maxTick, filters, ranges, from, size)
	return &entities.TransactionsResult{LastProcessedTick: maxTick, Hits: hits, Transactions: txs}, err
}
