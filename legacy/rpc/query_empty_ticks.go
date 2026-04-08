package rpc

import (
	"context"
	"fmt"
	"log"

	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
)

func (qs *QueryService) GetEmptyTicks(ctx context.Context, epoch uint32, intervals []*statusPb.TickInterval) (*EmptyTicks, error) {
	qs.emptyTicksLock.Lock() // costly and not threadsafe in case of update TODO use rwlock
	defer qs.emptyTicksLock.Unlock()

	emptyTicks := qs.cache.GetEmptyTicks(epoch)

	if emptyTicks != nil { // some sanity checks

		if emptyTicks.Epoch != epoch { // should not be possible (remove?)
			log.Printf("[ERROR] unexpected empty ticks data (expected data for epoch [%d], but got [%d]).", epoch, emptyTicks.Epoch)
			return nil, fmt.Errorf("illegal cache state for epoch [%d]", epoch)
		}

		// if len intervals == 0 (can happen on epoch change), then proceed with current empty ticks
		if len(intervals) != 0 && (intervals[0].Epoch != epoch || emptyTicks.StartTick != intervals[0].FirstTick) {
			log.Printf("[ERROR] Illegal argument. Empty ticks epoch [%d] / start [%d] / end [%d] / len [%d].",
				emptyTicks.Epoch, emptyTicks.StartTick, emptyTicks.EndTick, len(emptyTicks.Ticks))
			log.Printf("[ERROR] Illegal argument. Intervals: %v", intervals)
			return nil, fmt.Errorf("illegal state for epoch [%d]", epoch)
		}

		err := verifySorted(intervals)
		if err != nil {
			return nil, fmt.Errorf("unsorted intervals: %v", intervals)
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
	return emptyTicks, nil // TODO return copy instead of pointer
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
	}

	qs.ConsecutiveElasticErrorCount.Store(0)
	return ticks, nil
}

func verifySorted(intervals []*statusPb.TickInterval) error {
	tick := uint32(0)
	for _, interval := range intervals {
		if interval.FirstTick < tick {
			return fmt.Errorf("unsorted intervals: %v", intervals)
		}
		tick = interval.FirstTick
	}
	return nil
}
