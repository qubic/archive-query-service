package domain

import (
	"context"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/events.mock.go -package=mock -source events.go

type EventsRepository interface {
	GetEvents(ctx context.Context, filters map[string][]string, from, size uint32) ([]*api.Event, *entities.Hits, error)
}

type EventsService struct {
	repo EventsRepository
}

func NewEventsService(repo EventsRepository) *EventsService {
	return &EventsService{repo: repo}
}

func (s *EventsService) GetEvents(ctx context.Context, filters map[string][]string, from, size uint32) (*entities.EventsResult, error) {
	events, hits, err := s.repo.GetEvents(ctx, filters, from, size)
	if err != nil {
		return nil, err
	}
	return &entities.EventsResult{Hits: hits, Events: events}, nil
}
