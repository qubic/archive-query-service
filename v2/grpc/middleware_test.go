package grpc

import (
	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"testing"
)

func Test_WasSkippedByArchive(t *testing.T) {
	tcs := []struct {
		name                      string
		tick                      uint32
		intervals                 []*api.ProcessedTickInterval
		expectedSkipped           bool
		expectedNextAvailableTick uint32
	}{
		{
			name: "before first interval",
			tick: 10,
			intervals: []*api.ProcessedTickInterval{
				{
					FirstTick: 20,
					LastTick:  30,
				},
				{
					FirstTick: 31,
					LastTick:  40,
				},
			},
			expectedSkipped:           true,
			expectedNextAvailableTick: 20,
		},
		{
			name: "between first and second interval",
			tick: 31,
			intervals: []*api.ProcessedTickInterval{
				{
					FirstTick: 20,
					LastTick:  30,
				},
				{
					FirstTick: 40,
					LastTick:  50,
				},
			},
			expectedSkipped:           true,
			expectedNextAvailableTick: 40,
		},
		{
			name: "in first interval - first tick",
			tick: 20,
			intervals: []*api.ProcessedTickInterval{
				{
					FirstTick: 20,
					LastTick:  30,
				},
				{
					FirstTick: 40,
					LastTick:  50,
				},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in first interval - between first and last",
			tick: 25,
			intervals: []*api.ProcessedTickInterval{
				{
					FirstTick: 20,
					LastTick:  30,
				},
				{
					FirstTick: 40,
					LastTick:  50,
				},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in first interval - last tick",
			tick: 30,
			intervals: []*api.ProcessedTickInterval{
				{
					FirstTick: 20,
					LastTick:  30,
				},
				{
					FirstTick: 40,
					LastTick:  50,
				},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in last interval - first tick",
			tick: 40,
			intervals: []*api.ProcessedTickInterval{
				{
					FirstTick: 20,
					LastTick:  30,
				},
				{
					FirstTick: 40,
					LastTick:  50,
				},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in last interval - between first and last",
			tick: 45,
			intervals: []*api.ProcessedTickInterval{
				{
					FirstTick: 20,
					LastTick:  30,
				},
				{
					FirstTick: 40,
					LastTick:  50,
				},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in last interval - last tick",
			tick: 50,
			intervals: []*api.ProcessedTickInterval{
				{
					FirstTick: 20,
					LastTick:  30,
				},
				{
					FirstTick: 40,
					LastTick:  50,
				},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name:                      "nil tick interval", // shouldn't happen
			tick:                      50,
			intervals:                 nil,
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			skipped, nextTick := WasSkippedByArchive(tc.tick, tc.intervals)
			if skipped != tc.expectedSkipped {
				t.Errorf("expected skipped %v, got %v", tc.expectedSkipped, skipped)
			}
			if nextTick != tc.expectedNextAvailableTick {
				t.Errorf("expected next available tick %d, got %d", tc.expectedNextAvailableTick, nextTick)
			}
		})
	}
}
