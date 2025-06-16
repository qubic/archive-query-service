package domain

import (
	"context"
	"errors"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/repository.mock.go -package=mock -source transaction.go
type TransactionRepository interface {
	GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error)
	GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32) ([]*api.Transaction, error)
	GetTransactionsForIdentity(ctx context.Context, identity string, maxTick uint32, pageSize, pageNumber int, desc bool) ([]*api.Transaction, error)
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

func (s *TransactionService) GetTransactionsForIdentity(ctx context.Context, identity string, pageSize, pageNumber int, desc bool) ([]*api.Transaction, error) {
	maxTick, err := s.maxTickFetcher(ctx)
	if err != nil {
		return nil, err
	}

	if maxTick < 1 {
		return nil, nil // No transactions available
	}

	if pageNumber < 0 || pageSize <= 0 {
		return nil, errors.New("pageNumber and pageSize must be greater than 0")
	}

	if pageNumber*pageSize > int(maxTick) {
		return nil, errors.New("pageNumber and pageSize must be greater than or equal to maxTick")
	}

	return s.repo.GetTransactionsForIdentity(ctx, identity, maxTick, pageSize, pageNumber, desc)
}
