package rpc

import (
	"context"
	"fmt"
	"log"

	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
)

func (qs *QueryService) GetEmptyTicks(ctx context.Context, epoch uint32, intervals []*statusPb.TickInterval) (*EmptyTicks, error) {
	qs.emptyTicksLock.Lock() // costly and not threadsafe in case of update
	defer qs.emptyTicksLock.Unlock()

	emptyTicks := qs.cache.GetEmptyTicks(epoch)

	if emptyTicks != nil { // some sanity checks
		if len(intervals) == 0 || intervals[0].Epoch != epoch || emptyTicks.Epoch != epoch || emptyTicks.StartTick != intervals[0].FirstTick {
			log.Printf("[ERROR] Illegal argument. Empty ticks: %v", emptyTicks)
			log.Printf("[ERROR] Illegal argument. Intervals: %v", intervals)
			return nil, fmt.Errorf("illegal argument for epoch [%d]", epoch)
		}
		tick := uint32(0)
		for _, interval := range intervals {
			if interval.FirstTick < tick {
				return nil, fmt.Errorf("unsorted intervals: %v", intervals)
			}
			tick = interval.FirstTick
		}
	}

	if emptyTicks == nil { // reload

		var emptyTickList []uint32
		var startTick uint32
		var endTick uint32
		for _, interval := range intervals {
			if interval.Epoch == epoch {
				if startTick == 0 {
					startTick = interval.FirstTick
				}
				if endTick < interval.LastTick {
					endTick = interval.LastTick
				}
				ticks, err := qs.queryEmptyTicksFromElastic(ctx, interval.FirstTick, interval.LastTick, epoch)
				if err != nil {
					return nil, err
				}
				emptyTickList = append(emptyTickList, ticks...)
			}
		}
		tickMap := make(map[uint32]bool, len(emptyTickList))
		for _, tick := range emptyTickList {
			tickMap[tick] = true
		}
		emptyTicks = &EmptyTicks{
			Epoch:     epoch,
			StartTick: startTick,
			EndTick:   endTick,
			Ticks:     tickMap,
		}
		qs.cache.SetEmptyTicks(emptyTicks)

	} else { // add missing ticks if necessary. Needs lock as we operate on the cached value!

		for _, interval := range intervals {
			if interval.Epoch == epoch {
				if emptyTicks.EndTick < interval.LastTick {
					from := max(emptyTicks.EndTick+1, interval.FirstTick) // do no reload ticks we already have
					ticks, err := qs.queryEmptyTicksFromElastic(ctx, from, interval.LastTick, epoch)
					if err != nil {
						return nil, err
					}
					for _, tick := range ticks {
						emptyTicks.Ticks[tick] = true
					}
					emptyTicks.EndTick = interval.LastTick
				}
			}
		}
		qs.cache.SetEmptyTicks(emptyTicks) // not sure if this is necessary (update ttl, ...)

	}
	return emptyTicks, nil
}

func (qs *QueryService) queryEmptyTicksFromElastic(ctx context.Context, from, to uint32, epoch uint32) ([]uint32, error) {
	if to-from > 100 {
		log.Printf("[DEBUG] Query empty ticks: from [%d], to [%d], epoch [%d]", from, to, epoch)
	}
	ticks, err := qs.elasticClient.QueryEmptyTicks(ctx, from, to, epoch)
	if err != nil {
		qs.TotalElasticErrorCount.Add(1)
		qs.ConsecutiveElasticErrorCount.Add(1)
		return nil, fmt.Errorf("querying ticks from [%d] to [%d] in epoch [%d]: %w",
			from, to, epoch, err)
	} else {
		qs.ConsecutiveElasticErrorCount.Store(0)
	}
	return ticks, nil
}
