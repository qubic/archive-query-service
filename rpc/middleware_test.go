package rpc

import (
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"testing"
)

func Test_WasSkippedByArchive(t *testing.T) {
	tcs := []struct {
		name                      string
		tick                      uint32
		intervals                 []*statusPb.TickInterval
		expectedSkipped           bool
		expectedNextAvailableTick uint32
	}{
		{
			name: "before first interval",
			tick: 10,
			intervals: []*statusPb.TickInterval{
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
			intervals: []*statusPb.TickInterval{
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
			intervals: []*statusPb.TickInterval{
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
			intervals: []*statusPb.TickInterval{
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
			intervals: []*statusPb.TickInterval{
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
			intervals: []*statusPb.TickInterval{
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
			intervals: []*statusPb.TickInterval{
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
			intervals: []*statusPb.TickInterval{
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
