package domain

import (
	"context"
	"github.com/google/go-cmp/cmp"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/domain/mock"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/testing/protocmp"
	"testing"
	"time"
)

func Test_StatusService(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	statusServiceClient := mock.NewMockStatusServiceClient(ctrl)

	sc := NewStatusCache(statusServiceClient, 1*time.Second)
	statusServiceClient.EXPECT().GetStatus(gomock.Any(), gomock.Any()).Return(&statusPb.GetStatusResponse{
		LastProcessedTick: 12345,
	}, nil)

	statusSvc := NewStatusService(sc)

	// Test GetLastProcessedTick
	t.Run("GetLastProcessedTick", func(t *testing.T) {
		resp, err := statusSvc.GetLastProcessedTick(ctx)
		require.NoError(t, err, "getting last processed tick")

		expected := &api.LastProcessedTick{TickNumber: 12345}
		require.Equal(t, expected, resp, "expected last processed tick to be 12345")
	})

	statusServiceClient.EXPECT().GetTickIntervals(gomock.Any(), gomock.Any()).Return(&statusPb.GetTickIntervalsResponse{
		Intervals: []*statusPb.TickInterval{
			{
				Epoch:     12,
				FirstTick: 1,
				LastTick:  10,
			},
			{
				Epoch:     13,
				FirstTick: 15,
				LastTick:  20,
			},
		},
	}, nil)

	// Test GetProcessedTickIntervals
	t.Run("GetProcessedTickIntervals", func(t *testing.T) {
		resp, err := statusSvc.GetProcessedTickIntervals(ctx)
		require.NoError(t, err, "getting processed tick intervals")
		expected := []*api.ProcessedTickInterval{
			{
				Epoch:     12,
				FirstTick: 1,
				LastTick:  10,
			},
			{
				Epoch:     13,
				FirstTick: 15,
				LastTick:  20,
			},
		}

		diff := cmp.Diff(expected, resp, protocmp.Transform())
		require.Empty(t, diff, "expected processed tick intervals to match", "diff: %s", diff)
	})
}
