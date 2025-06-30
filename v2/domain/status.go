package domain

import (
	"context"
	"fmt"
	"github.com/jellydator/ttlcache/v3"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"golang.org/x/sync/singleflight"
	"time"
)

//go:generate go tool go.uber.org/mock/mockgen -destination=mock/gprc_status.mock.go -package=mock github.com/qubic/go-data-publisher/status-service/protobuf StatusServiceClient

const maxTickCacheKey = "max_tick"
const tickIntervalsCacheKey = "tick_intervals"

type StatusCache struct {
	lastProcessedTickProvider *ttlcache.Cache[string, uint32]
	tickIntervalsProvider     *ttlcache.Cache[string, []*statusPb.TickInterval]
	StatusServiceClient       statusPb.StatusServiceClient
	sfGroup                   *singleflight.Group
}

func NewStatusCache(statusServiceClient statusPb.StatusServiceClient, ttl time.Duration) *StatusCache {
	lastProcessedTickProvider := ttlcache.New[string, uint32](
		ttlcache.WithTTL[string, uint32](ttl),
		ttlcache.WithDisableTouchOnHit[string, uint32](), // don't refresh ttl upon getting the item from cache
	)

	tickIntervalsProvider := ttlcache.New[string, []*statusPb.TickInterval](
		ttlcache.WithTTL[string, []*statusPb.TickInterval](ttl),
		ttlcache.WithDisableTouchOnHit[string, []*statusPb.TickInterval](), // don't refresh ttl upon getting the item from cache
	)
	return &StatusCache{
		lastProcessedTickProvider: lastProcessedTickProvider,
		tickIntervalsProvider:     tickIntervalsProvider,
		StatusServiceClient:       statusServiceClient,
		sfGroup:                   &singleflight.Group{},
	}
}

func (s *StatusCache) GetMaxTick(ctx context.Context) (uint32, error) {
	maxTick, err, _ := s.sfGroup.Do(maxTickCacheKey, func() (interface{}, error) {
		// Check if the max tick is already cached
		if item := s.lastProcessedTickProvider.Get(maxTickCacheKey); item != nil {
			return item.Value(), nil
		}

		// If not cached, fetch from the status service
		maxTick, err := s.fetchStatusMaxTick(ctx)
		if err != nil {
			return 0, fmt.Errorf("fetching status service max tick: %v", err)
		}

		// Cache the fetched max tick
		s.lastProcessedTickProvider.Set(maxTickCacheKey, maxTick, ttlcache.DefaultTTL)
		return maxTick, nil
	})
	if err != nil {
		return 0, fmt.Errorf("getting max tick from cache: %v", err)
	}

	// cast to uint32
	maxTickUint32, ok := maxTick.(uint32)
	if !ok {
		return 0, fmt.Errorf("invalid type assertion for max tick: expected uint32, got %T", maxTick)
	}

	return maxTickUint32, nil
}

func (s *StatusCache) GetTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	tickIntervals, err, _ := s.sfGroup.Do(tickIntervalsCacheKey, func() (interface{}, error) {
		item := s.tickIntervalsProvider.Get(tickIntervalsCacheKey)
		if item != nil {
			return item.Value(), nil
		}

		tickIntervals, err := s.fetchTickIntervals(ctx)
		if err != nil {
			return nil, fmt.Errorf("fetching status service tick intervals: %v", err)
		}

		s.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
		return tickIntervals, nil
	})
	if err != nil {
		return nil, fmt.Errorf("getting tick intervals from cache: %v", err)
	}

	// cast to []*statusPb.TickInterval
	tickIntervalsSlice, ok := tickIntervals.([]*statusPb.TickInterval)
	if !ok {
		return nil, fmt.Errorf("invalid type assertion for tick intervals: expected []*statusPb.TickInterval, got %T", tickIntervals)
	}

	return tickIntervalsSlice, nil
}

func (s *StatusCache) Start() {
	s.lastProcessedTickProvider.Start()
	s.tickIntervalsProvider.Start()
}

func (s *StatusCache) Stop() {
	s.lastProcessedTickProvider.Stop()
	s.tickIntervalsProvider.Stop()
}

func (s *StatusCache) fetchStatusMaxTick(ctx context.Context) (uint32, error) {
	statusResponse, err := s.StatusServiceClient.GetStatus(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("fetching status service: %v", err)
	}

	return statusResponse.LastProcessedTick, nil
}

func (s *StatusCache) fetchTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	tickIntervalsResponse, err := s.StatusServiceClient.GetTickIntervals(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("fetching tick intervals: %v", err)
	}

	if len(tickIntervalsResponse.Intervals) == 0 {
		return nil, fmt.Errorf("no tick intervals found")
	}

	return tickIntervalsResponse.Intervals, nil
}

type StatusService struct {
	cache *StatusCache
}

func NewStatusService(cache *StatusCache) *StatusService {
	return &StatusService{
		cache: cache,
	}
}

func (s *StatusService) GetLastProcessedTick(ctx context.Context) (*api.LastProcessedTick, error) {
	maxTick, err := s.cache.GetMaxTick(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting max tick from cache: %v", err)
	}

	return &api.LastProcessedTick{TickNumber: maxTick}, nil
}

func (s *StatusService) GetProcessedTickIntervals(ctx context.Context) ([]*api.ProcessedTickInterval, error) {
	tickIntervals, err := s.cache.GetTickIntervals(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting tick intervals: %v", err)
	}

	return toApiProcessedTickIntervals(tickIntervals), nil
}

func toApiProcessedTickIntervals(source []*statusPb.TickInterval) []*api.ProcessedTickInterval {
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
