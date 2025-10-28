package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/jellydator/ttlcache/v3"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"golang.org/x/sync/singleflight"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/gprc_status.mock.go -package=mock github.com/qubic/go-data-publisher/status-service/protobuf StatusServiceClient

const statusCacheKey = "status"
const tickIntervalsCacheKey = "tick_intervals"

type StatusGetter struct {
	statusProviderCache *ttlcache.Cache[string, *statusPb.GetStatusResponse]
	tiProviderCache     *ttlcache.Cache[string, []*statusPb.TickInterval]
	StatusServiceClient statusPb.StatusServiceClient
	sfGroup             *singleflight.Group
}

func NewStatusGetter(statusServiceClient statusPb.StatusServiceClient, cacheTTL time.Duration) *StatusGetter {
	statusProvider := ttlcache.New[string, *statusPb.GetStatusResponse](
		ttlcache.WithTTL[string, *statusPb.GetStatusResponse](cacheTTL),
		ttlcache.WithDisableTouchOnHit[string, *statusPb.GetStatusResponse](), // don't refresh cacheTTL upon getting the item from getter
	)

	tickIntervalsProvider := ttlcache.New[string, []*statusPb.TickInterval](
		ttlcache.WithTTL[string, []*statusPb.TickInterval](cacheTTL),
		ttlcache.WithDisableTouchOnHit[string, []*statusPb.TickInterval](), // don't refresh cacheTTL upon getting the item from getter
	)
	return &StatusGetter{
		statusProviderCache: statusProvider,
		tiProviderCache:     tickIntervalsProvider,
		StatusServiceClient: statusServiceClient,
		sfGroup:             &singleflight.Group{},
	}
}

func (s *StatusGetter) GetStatus(ctx context.Context) (*statusPb.GetStatusResponse, error) {
	cachedStatus, err, _ := s.sfGroup.Do(statusCacheKey, func() (interface{}, error) {
		// Check if the status is already cached
		if item := s.statusProviderCache.Get(statusCacheKey); item != nil {
			return item.Value(), nil
		}

		// If not cached, fetch from the status service
		status, err := s.fetchStatus(ctx)
		if err != nil {
			return 0, fmt.Errorf("fetching status service status: %w", err)
		}

		// Cache the fetched status
		s.statusProviderCache.Set(statusCacheKey, status, ttlcache.DefaultTTL)
		return status, nil
	})
	if err != nil {
		return nil, fmt.Errorf("getting status from getter: %w", err)
	}

	// cast to object pointer
	status, ok := cachedStatus.(*statusPb.GetStatusResponse)
	if !ok {
		return nil, fmt.Errorf("invalid type assertion for status: expected *statusPb.GetStatusResponse, got %T", status)
	}

	return status, nil
}

func (s *StatusGetter) GetTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	tickIntervals, err, _ := s.sfGroup.Do(tickIntervalsCacheKey, func() (interface{}, error) {
		item := s.tiProviderCache.Get(tickIntervalsCacheKey)
		if item != nil {
			return item.Value(), nil
		}

		tickIntervals, err := s.fetchTickIntervals(ctx)
		if err != nil {
			return nil, fmt.Errorf("fetching status service tick intervals: %w", err)
		}

		s.tiProviderCache.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
		return tickIntervals, nil
	})
	if err != nil {
		return nil, fmt.Errorf("getting tick intervals from getter: %w", err)
	}

	// cast to []*statusPb.TickInterval
	tickIntervalsSlice, ok := tickIntervals.([]*statusPb.TickInterval)
	if !ok {
		return nil, fmt.Errorf("invalid type assertion for tick intervals: expected []*statusPb.TickInterval, got %T", tickIntervals)
	}

	return tickIntervalsSlice, nil
}

func (s *StatusGetter) Start() {
	s.statusProviderCache.Start()
	s.tiProviderCache.Start()
}

func (s *StatusGetter) Stop() {
	s.statusProviderCache.Stop()
	s.tiProviderCache.Stop()
}

func (s *StatusGetter) fetchStatus(ctx context.Context) (*statusPb.GetStatusResponse, error) {
	statusResponse, err := s.StatusServiceClient.GetStatus(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("fetching status service from grpc service: %w", err)
	}
	return statusResponse, nil
}

func (s *StatusGetter) fetchTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	tickIntervalsResponse, err := s.StatusServiceClient.GetTickIntervals(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("fetching tick intervals from grpc service: %w", err)
	}

	if len(tickIntervalsResponse.Intervals) == 0 {
		return nil, fmt.Errorf("no tick intervals found")
	}

	return tickIntervalsResponse.Intervals, nil
}

type StatusService struct {
	getter *StatusGetter
}

func NewStatusService(getter *StatusGetter) *StatusService {
	return &StatusService{
		getter: getter,
	}
}

func (s *StatusService) GetStatus(ctx context.Context) (*statusPb.GetStatusResponse, error) {
	status, err := s.getter.GetStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting status: %w", err)
	}

	return status, nil
}

func (s *StatusService) GetProcessedTickIntervals(ctx context.Context) ([]*api.ProcessedTickInterval, error) {
	tickIntervals, err := s.getter.GetTickIntervals(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting tick intervals: %w", err)
	}

	return toAPIProcessedTickIntervals(tickIntervals), nil
}

func toAPIProcessedTickIntervals(source []*statusPb.TickInterval) []*api.ProcessedTickInterval {
	intervals := make([]*api.ProcessedTickInterval, 0, len(source))

	for _, interval := range source {
		intervals = append(intervals, &api.ProcessedTickInterval{
			Epoch:     interval.Epoch,
			FirstTick: interval.FirstTick,
			LastTick:  interval.LastTick,
		})
	}

	return intervals
}
