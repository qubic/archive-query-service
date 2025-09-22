package domain

import (
	"context"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/computors.mock.go -package=mock -source computors.go
type ComputorsListRepository interface {
	GetComputorsListsForEpoch(ctx context.Context, epoch uint32) ([]*api.ComputorsList, error)
}

type ComputorsListService struct {
	repo ComputorsListRepository
}

func NewComputorsListService(repo ComputorsListRepository) *ComputorsListService {
	return &ComputorsListService{
		repo: repo,
	}
}

func (s *ComputorsListService) GetComputorsListsForEpoch(ctx context.Context, epoch uint32) ([]*api.ComputorsList, error) {
	return s.repo.GetComputorsListsForEpoch(ctx, epoch)
}
