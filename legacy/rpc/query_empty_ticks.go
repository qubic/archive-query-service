package rpc

import (
	"context"
	"fmt"
	"log"

	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
)

const emptyTickQueryOffset = 3

func (qs *QueryService) GetEmptyTicks(ctx context.Context, epoch uint32, intervals []*statusPb.TickInterval) (*EmptyTicks, error) {
	qs.emptyTicksLock.Lock() // costly and not threadsafe in case of update TODO use rwlock
	defer qs.emptyTicksLock.Unlock()

	emptyTicks := qs.cache.GetEmptyTicks(epoch)          // only used here
	err := sanityCheckData(emptyTicks, intervals, epoch) // checks intervals and empty ticks
	if err != nil {
		return nil, err
	}

	if emptyTicks == nil { // reload

		var emptyTickList []uint32
		var startTick uint32
		var endTick uint32
		for _, interval := range intervals {
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

		for i, interval := range intervals {
			if emptyTicks.EndTick < interval.LastTick {
				from, to := calculateRange(emptyTicks, interval, i, len(intervals))
				ticks, err := qs.queryEmptyTicksFromElastic(ctx, from, to, epoch)
				if err != nil {
					return nil, err
				}
				for _, tick := range ticks {
					emptyTicks.Ticks[tick] = true
				}
				emptyTicks.EndTick = to
			}
		}

	}
	return emptyTicks.Clone(), nil // Return deep copy
}

func calculateRange(emptyTicks *EmptyTicks, interval *statusPb.TickInterval, index, len int) (uint32, uint32) {
	from := max(emptyTicks.EndTick+1, interval.FirstTick) // do no reload ticks we already have
	to := interval.LastTick
	if isLast(index, len) && isFirstCall(emptyTicks, interval) {
		to = max(from, interval.LastTick-emptyTickQueryOffset)
	}
	return from, to
}

func isFirstCall(emptyTicks *EmptyTicks, interval *statusPb.TickInterval) bool {
	return emptyTicks.EndTick < interval.LastTick-emptyTickQueryOffset
}

func isLast(index int, len int) bool {
	return index == len-1
}

func (qs *QueryService) queryEmptyTicksFromElastic(ctx context.Context, from, to, epoch uint32) ([]uint32, error) {
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

func sanityCheckData(emptyTicks *EmptyTicks, intervals []*statusPb.TickInterval, epoch uint32) error {
	err := verifySortedInEpoch(intervals, epoch)
	if err != nil {
		return err
	}

	if emptyTicks != nil {
		if emptyTicks.Epoch != epoch { // should not be possible (remove?)
			log.Printf("[ERROR] unexpected empty ticks data (expected data for epoch [%d], but got [%d]).", epoch, emptyTicks.Epoch)
			return fmt.Errorf("illegal cache state for epoch [%d]", epoch)
		}

		// if len intervals == 0 (can happen on epoch change), then proceed with current empty ticks
		if len(intervals) != 0 && (intervals[0].Epoch != epoch || emptyTicks.StartTick != intervals[0].FirstTick) {
			log.Printf("[ERROR] Illegal argument. Empty ticks epoch [%d] / start [%d] / end [%d] / len [%d].",
				emptyTicks.Epoch, emptyTicks.StartTick, emptyTicks.EndTick, len(emptyTicks.Ticks))
			log.Printf("[ERROR] Illegal argument. Intervals: %v", intervals)
			return fmt.Errorf("illegal state for epoch [%d]", epoch)
		}
	}

	return nil
}

func verifySortedInEpoch(intervals []*statusPb.TickInterval, epoch uint32) error {
	tick := uint32(0)
	for _, interval := range intervals {
		if interval.Epoch != epoch {
			return fmt.Errorf("got interval for wrong epoch [%d] (expected: [%d])", interval.Epoch, epoch)
		}
		if interval.FirstTick < tick {
			return fmt.Errorf("unsorted intervals: %v", intervals)
		}
		tick = interval.FirstTick
	}
	return nil
}
