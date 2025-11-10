package domain

import (
	"context"
	"errors"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/tickdata.mock.go -package=mock -source tickdata.go
type TickDataRepository interface {
	GetTickData(ctx context.Context, tickNumber uint32) (*api.TickData, error)
}

type TickDataService struct {
	repo TickDataRepository
}

func NewTickDataService(repo TickDataRepository) *TickDataService {
	return &TickDataService{
		repo: repo,
	}
}

// GetTickData Returns the tick data or nil, if no tick data is found.
func (s *TickDataService) GetTickData(ctx context.Context, tickNumber uint32) (*api.TickData, error) {
	tickData, err := s.repo.GetTickData(ctx, tickNumber)
	if errors.Is(err, ErrNotFound) { // handle empty tick.
		return nil, nil
	} // empty tick
	return tickData, err
}
