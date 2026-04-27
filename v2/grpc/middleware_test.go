package grpc

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/grpc/mock"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTickWithinBoundsInterceptor_checkTickWithinArchiverIntervals(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStatusService := mock.NewMockStatusService(ctrl)
	interceptor := NewTickWithinBoundsInterceptor(mockStatusService)

	tcs := []struct {
		name              string
		tickNumber        uint32
		lastProcessedTick uint32
		intervals         []*api.ProcessedTickInterval
		expectedCode      codes.Code
	}{
		{
			name:              "tick greater than last processed tick",
			tickNumber:        101,
			lastProcessedTick: 100,
			intervals:         nil,                      // not reached
			expectedCode:      codes.FailedPrecondition, // changing this breaks backwards compatibility
		},
		{
			name:              "tick skipped by archive",
			tickNumber:        15,
			lastProcessedTick: 100,
			intervals: []*api.ProcessedTickInterval{
				{FirstTick: 20, LastTick: 30},
				{FirstTick: 40, LastTick: 50},
			},
			expectedCode: codes.OutOfRange, // changing this breaks backwards compatibility
		},
		{
			name:              "tick within valid interval",
			tickNumber:        25,
			lastProcessedTick: 100,
			intervals: []*api.ProcessedTickInterval{
				{FirstTick: 20, LastTick: 30},
			},
			expectedCode: codes.OK,
		},
	}

	for _, tc := range tcs {
		tc := tc // Capture range variable
		t.Run(tc.name, func(t *testing.T) {
			mockStatusService.EXPECT().GetStatus(gomock.Any()).Return(&statusPb.GetStatusResponse{
				LastProcessedTick: tc.lastProcessedTick,
			}, nil)

			mockStatusService.EXPECT().GetProcessedTickIntervals(gomock.Any()).Return(tc.intervals, nil)

			err := interceptor.checkTickWithinArchiverIntervals(context.Background(), tc.tickNumber)
			if tc.expectedCode == codes.OK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, tc.expectedCode, st.Code())
			}
		})
	}
}

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
				{FirstTick: 20, LastTick: 30},
				{FirstTick: 31, LastTick: 40},
			},
			expectedSkipped:           true,
			expectedNextAvailableTick: 20,
		},
		{
			name: "between first and second interval",
			tick: 31,
			intervals: []*api.ProcessedTickInterval{
				{FirstTick: 20, LastTick: 30},
				{FirstTick: 40, LastTick: 50},
			},
			expectedSkipped:           true,
			expectedNextAvailableTick: 40,
		},
		{
			name: "in first interval - first tick",
			tick: 20,
			intervals: []*api.ProcessedTickInterval{
				{FirstTick: 20, LastTick: 30},
				{FirstTick: 40, LastTick: 50},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in first interval - between first and last",
			tick: 25,
			intervals: []*api.ProcessedTickInterval{
				{FirstTick: 20, LastTick: 30},
				{FirstTick: 40, LastTick: 50},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in first interval - last tick",
			tick: 30,
			intervals: []*api.ProcessedTickInterval{
				{FirstTick: 20, LastTick: 30},
				{FirstTick: 40, LastTick: 50},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in last interval - first tick",
			tick: 40,
			intervals: []*api.ProcessedTickInterval{
				{FirstTick: 20, LastTick: 30},
				{FirstTick: 40, LastTick: 50},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in last interval - between first and last",
			tick: 45,
			intervals: []*api.ProcessedTickInterval{
				{FirstTick: 20, LastTick: 30},
				{FirstTick: 40, LastTick: 50},
			},
			expectedSkipped:           false,
			expectedNextAvailableTick: 0,
		},
		{
			name: "in last interval - last tick",
			tick: 50,
			intervals: []*api.ProcessedTickInterval{
				{FirstTick: 20, LastTick: 30},
				{FirstTick: 40, LastTick: 50},
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

func Test_createTTLMapFromJSONFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "ttlmap-*.json")
	require.NoError(t, err, "could not create temp file")
	defer os.Remove(tmpFile.Name())

	content := `{
		"endpointA": "5m",
		"endpointB": "1h",
		"endpointC": "30s"
	}`
	_, err = tmpFile.Write([]byte(content))
	require.NoError(t, err, "could not write to temp file")
	tmpFile.Close()

	ttlMap, err := CreateTTLMapFromJSONFile(tmpFile.Name())
	require.NoError(t, err, "could not create TTLMap from JSON file")

	expected := map[string]time.Duration{
		"endpointA": 5 * time.Minute,
		"endpointB": 1 * time.Hour,
		"endpointC": 30 * time.Second,
	}

	diff := cmp.Diff(ttlMap, expected)
	require.Empty(t, diff)

	tmpFile, err = os.CreateTemp("", "ttlmap-*.json")
	require.NoError(t, err, "could not create temp file")
	defer os.Remove(tmpFile.Name())

	content = `{
		"endpointA": "500",
		"endpointB": "1h",
		"endpointC": "30s"
	}`
	_, err = tmpFile.Write([]byte(content))
	require.NoError(t, err, "could not write to temp file")
	tmpFile.Close()

	_, err = CreateTTLMapFromJSONFile(tmpFile.Name()) // nolint:ineffassign
	require.Error(t, err)
}
