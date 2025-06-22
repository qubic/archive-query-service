package domain

import (
	"context"
	"fmt"
	"github.com/jellydator/ttlcache/v3"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"time"
)

const maxTickCacheKey = "max_tick"
const tickIntervalsCacheKey = "tick_intervals"

type StatusCache struct {
	lastProcessedTickProvider *ttlcache.Cache[string, uint32]
	tickIntervalsProvider     *ttlcache.Cache[string, []*statusPb.TickInterval]
	StatusServiceClient       statusPb.StatusServiceClient
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
	}
}

func (s *StatusCache) GetMaxTick(ctx context.Context) (uint32, error) {
	if s.lastProcessedTickProvider.Has(maxTickCacheKey) {
		item := s.lastProcessedTickProvider.Get(maxTickCacheKey)
		if item != nil {
			return item.Value(), nil
		}
	}

	maxTick, err := s.fetchStatusMaxTick(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetching status service max tick: %v", err)
	}

	s.lastProcessedTickProvider.Set(maxTickCacheKey, maxTick, ttlcache.DefaultTTL)
	return maxTick, nil
}

func (s *StatusCache) GetTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	if s.tickIntervalsProvider.Has(tickIntervalsCacheKey) {
		item := s.tickIntervalsProvider.Get(tickIntervalsCacheKey)
		if item != nil {
			return item.Value(), nil
		}
	}

	tickIntervals, err := s.fetchTickIntervals(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching status service max tick: %v", err)
	}

	s.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	return tickIntervals, nil
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

func (s *StatusService) GetLastProcessedTick(ctx context.Context) (*api.GetLastProcessedTickResponse, error) {
	maxTick, err := s.cache.GetMaxTick(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting max tick from cache: %v", err)
	}

	return &api.GetLastProcessedTickResponse{TickNumber: maxTick}, nil
}

func (s *StatusService) GetProcessedTickIntervals(ctx context.Context) (*api.GetProcessedTicksIntervalsResponse, error) {
	tickIntervals, err := s.cache.GetTickIntervals(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting tick intervals: %v", err)
	}

	return &api.GetProcessedTicksIntervalsResponse{ProcessedTicksIntervals: toApiProcessedTickIntervals(tickIntervals)}, nil
}

func toApiProcessedTickIntervals(source []*statusPb.TickInterval) []*api.ProcessedTickInterval {
	intervals := make([]*api.ProcessedTickInterval, len(source))

	for _, interval := range source {
		intervals = append(intervals, &api.ProcessedTickInterval{
			Epoch:     interval.Epoch,
			FirstTick: interval.FirstTick,
			LastTick:  interval.LastTick,
		})
	}

	return intervals
}
