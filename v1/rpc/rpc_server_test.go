package rpc

import (
	"testing"

	"github.com/qubic/archive-query-service/protobuf"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRpcServer_ConvertArchiverStatus(t *testing.T) {

	// response from status service
	sourceStatus := &statusPb.GetArchiverStatusResponse{
		LastProcessedTick: &statusPb.ProcessedTick{
			TickNumber: 12345,
			Epoch:      123,
		},
		ProcessedTickIntervalsPerEpoch: []*statusPb.ProcessedTickIntervalsPerEpoch{
			{
				Epoch: 100,
				Intervals: []*statusPb.ProcessedTickInterval{
					{
						InitialProcessedTick: 1,
						LastProcessedTick:    1000,
					},
				},
			},
			{
				Epoch: 123,
				Intervals: []*statusPb.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000,
						LastProcessedTick:    123456,
					},
				},
			},
		},
	}

	// expected response from query service
	expectedStatus := &protobuf.GetArchiverStatusResponse{
		LastProcessedTick: &protobuf.ProcessedTick{
			TickNumber: 12345,
			Epoch:      123,
		},
		ProcessedTickIntervalsPerEpoch: []*protobuf.ProcessedTickIntervalsPerEpoch{
			{
				Epoch: 100,
				Intervals: []*protobuf.ProcessedTickInterval{
					{
						InitialProcessedTick: 1,
						LastProcessedTick:    1000,
					},
				},
			},
			{
				Epoch: 123,
				Intervals: []*protobuf.ProcessedTickInterval{
					{
						InitialProcessedTick: 10000,
						LastProcessedTick:    123456,
					},
				},
			},
		},
	}

	convertedStatus, err := convertArchiverStatus(sourceStatus)
	require.NoError(t, err)
	assert.True(t, proto.Equal(expectedStatus, convertedStatus))

}
