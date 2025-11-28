package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/jellydator/ttlcache/v3"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
)

const maxTickCacheKey = "max_tick"
const tickIntervalsCacheKey = "tick_intervals"
const emptyTicksCacheKeyFormat = "empty_ticks_%d"

type EmptyTicks struct {
	Epoch     uint32
	StartTick uint32
	EndTick   uint32
	Ticks     map[uint32]bool
}

type StatusCache struct {
	lastProcessedTickProvider *ttlcache.Cache[string, uint32]
	tickIntervalsProvider     *ttlcache.Cache[string, []*statusPb.TickInterval]
	emptyTicksCache           *ttlcache.Cache[string, *EmptyTicks]
	StatusServiceClient       statusPb.StatusServiceClient
}

func NewStatusCache(statusServiceClient statusPb.StatusServiceClient, emptyTicksTTL, ttl time.Duration) *StatusCache {
	lastProcessedTickProvider := ttlcache.New[string, uint32](
		ttlcache.WithTTL[string, uint32](ttl),
		ttlcache.WithDisableTouchOnHit[string, uint32](),
	)

	tickIntervalsProvider := ttlcache.New[string, []*statusPb.TickInterval](
		ttlcache.WithTTL[string, []*statusPb.TickInterval](ttl),
		ttlcache.WithDisableTouchOnHit[string, []*statusPb.TickInterval](),
	)

	emptyTicksCache := ttlcache.New[string, *EmptyTicks](
		ttlcache.WithTTL[string, *EmptyTicks](emptyTicksTTL),
	)

	return &StatusCache{
		emptyTicksCache:           emptyTicksCache,
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
		return 0, fmt.Errorf("fetching status service max tick: %w", err)
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
		return nil, fmt.Errorf("fetching status service max tick: %w", err)
	}

	s.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	return tickIntervals, nil
}

func (s *StatusCache) GetEmptyTicks(epoch uint32) *EmptyTicks {
	key := fmt.Sprintf(emptyTicksCacheKeyFormat, epoch)
	if s.emptyTicksCache.Has(key) {
		item := s.emptyTicksCache.Get(key)
		if item != nil {
			return item.Value()
		}
	}
	return nil
}

func (s *StatusCache) SetEmptyTicks(ticks *EmptyTicks) {
	key := fmt.Sprintf(emptyTicksCacheKeyFormat, ticks.Epoch)
	s.emptyTicksCache.Set(key, ticks, ttlcache.DefaultTTL)
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
		return 0, fmt.Errorf("fetching status service: %w", err)
	}

	return statusResponse.LastProcessedTick, nil
}

func (s *StatusCache) fetchTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	tickIntervalsResponse, err := s.StatusServiceClient.GetTickIntervals(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("fetching tick intervals: %w", err)
	}

	if len(tickIntervalsResponse.Intervals) == 0 {
		return nil, fmt.Errorf("no tick intervals found")
	}

	return tickIntervalsResponse.Intervals, nil
}
