package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/qubic/archive-query-service/legacy/protobuf"
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

func TestRpcServer_QueryEmptyTicks_GivenAscendingTrue_ReturnInAscendingOrder(t *testing.T) {
	tickIntervals := []*statusPb.TickInterval{
		{
			Epoch:     123,
			FirstTick: 1,
			LastTick:  1000,
		},
	}

	elasticClient := &FakeElasticClient{
		emptyTicks: map[string][]uint32{"123-1-1000": {1, 2, 3, 7, 500, 999, 1000}},
	}

	statusCache := NewStatusCache(nil, time.Minute, time.Minute)
	statusCache.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	qs := &QueryService{
		elasticClient: elasticClient,
		cache:         statusCache,
	}

	server := Server{
		qb:            qs,
		statusService: nil,
	}
	response, err := server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{
		Epoch: 123,
		Desc:  false,
	})
	require.NoError(t, err)
	require.NotNil(t, response)

	expectedTicks := []*protobuf.Tick{
		{TickNumber: 1, IsEmpty: true},
		{TickNumber: 2, IsEmpty: true},
		{TickNumber: 3, IsEmpty: true},
		{TickNumber: 4, IsEmpty: false},
		{TickNumber: 5, IsEmpty: false},
		{TickNumber: 6, IsEmpty: false},
		{TickNumber: 7, IsEmpty: true},
		{TickNumber: 8, IsEmpty: false},
		{TickNumber: 9, IsEmpty: false},
		{TickNumber: 10, IsEmpty: false},
	}

	assert.Equal(t, expectedTicks, response.Ticks)
}

func TestRpcServer_QueryEmptyTicks_GivenAscendingTrue_ReturnInDescendingOrder(t *testing.T) {
	tickIntervals := []*statusPb.TickInterval{
		{
			Epoch:     123,
			FirstTick: 1,
			LastTick:  1000,
		},
	}

	elasticClient := &FakeElasticClient{
		emptyTicks: map[string][]uint32{"123-1-1000": {1, 2, 3, 7, 500, 999, 1000}},
	}

	statusCache := NewStatusCache(nil, time.Minute, time.Minute)
	statusCache.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	qs := &QueryService{
		elasticClient: elasticClient,
		cache:         statusCache,
	}

	server := Server{
		qb:            qs,
		statusService: nil,
	}

	response, err := server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{
		Epoch: 123,
		Desc:  true,
	})
	require.NoError(t, err)
	require.NotNil(t, response)

	expectedTicks := []*protobuf.Tick{
		{TickNumber: 1000, IsEmpty: true},
		{TickNumber: 999, IsEmpty: true},
		{TickNumber: 998, IsEmpty: false},
		{TickNumber: 997, IsEmpty: false},
		{TickNumber: 996, IsEmpty: false},
		{TickNumber: 995, IsEmpty: false},
		{TickNumber: 994, IsEmpty: false},
		{TickNumber: 993, IsEmpty: false},
		{TickNumber: 992, IsEmpty: false},
		{TickNumber: 991, IsEmpty: false},
	}

	assert.Equal(t, expectedTicks, response.Ticks)
}

func TestRpcServer_QueryEmptyTicks_GivenMultipleIntervals_ReturnPageOverIntervalBorders(t *testing.T) {
	tickIntervals := []*statusPb.TickInterval{
		{
			Epoch:     123,
			FirstTick: 1,
			LastTick:  100,
		},
		{
			Epoch:     123,
			FirstTick: 200,
			LastTick:  300,
		},
	}

	elasticClient := &FakeElasticClient{
		emptyTicks: map[string][]uint32{"123-1-100": {1, 3, 95, 100}, "123-200-300": {200, 202, 300}},
	}

	statusCache := NewStatusCache(nil, time.Minute, time.Minute)
	statusCache.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	qs := &QueryService{
		elasticClient: elasticClient,
		cache:         statusCache,
	}

	server := Server{
		qb:            qs,
		statusService: nil,
	}

	response, err := server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{
		Epoch:    123,
		Page:     1,
		PageSize: 1000,
		Desc:     true,
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Ticks, 201)

	assert.Equal(t, &protobuf.Tick{TickNumber: 300, IsEmpty: true}, response.Ticks[0])
	assert.Equal(t, &protobuf.Tick{TickNumber: 202, IsEmpty: true}, response.Ticks[98])
	assert.Equal(t, &protobuf.Tick{TickNumber: 200, IsEmpty: true}, response.Ticks[100])
	assert.Equal(t, &protobuf.Tick{TickNumber: 100, IsEmpty: true}, response.Ticks[101])
	assert.Equal(t, &protobuf.Tick{TickNumber: 99, IsEmpty: false}, response.Ticks[102])
	assert.Equal(t, &protobuf.Tick{TickNumber: 1, IsEmpty: true}, response.Ticks[200])

}

func TestRpcServer_QueryEmptyTicks_GivenMultipleIntervals_ReturnCorrectPages(t *testing.T) {
	tickIntervals := []*statusPb.TickInterval{
		{
			Epoch:     123,
			FirstTick: 1,
			LastTick:  100,
		},
		{
			Epoch:     123,
			FirstTick: 200,
			LastTick:  300,
		},
	}

	elasticClient := &FakeElasticClient{
		emptyTicks: map[string][]uint32{"123-1-100": {1, 3, 95, 100}, "123-200-300": {200, 202, 300}},
	}

	statusCache := NewStatusCache(nil, time.Minute, time.Minute)
	statusCache.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	qs := &QueryService{
		elasticClient: elasticClient,
		cache:         statusCache,
	}

	server := Server{
		qb:            qs,
		statusService: nil,
	}

	response, err := server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{
		Epoch:    123,
		Page:     1,
		PageSize: 60,
		Desc:     true,
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Ticks, 60)

	require.Equal(t, &protobuf.Tick{TickNumber: 300, IsEmpty: true}, response.Ticks[0])
	require.Equal(t, &protobuf.Tick{TickNumber: 241, IsEmpty: false}, response.Ticks[59])

	response, err = server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{
		Epoch:    123,
		Page:     2,
		PageSize: 60,
		Desc:     true,
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Ticks, 60)

	require.Equal(t, &protobuf.Tick{TickNumber: 240, IsEmpty: false}, response.Ticks[0])
	require.Equal(t, &protobuf.Tick{TickNumber: 200, IsEmpty: true}, response.Ticks[40]) // 41
	require.Equal(t, &protobuf.Tick{TickNumber: 100, IsEmpty: true}, response.Ticks[41]) // 42
	require.Equal(t, &protobuf.Tick{TickNumber: 95, IsEmpty: true}, response.Ticks[46])
	require.Equal(t, &protobuf.Tick{TickNumber: 82, IsEmpty: false}, response.Ticks[59]) // 60

	response, err = server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{
		Epoch:    123,
		Page:     3,
		PageSize: 60,
		Desc:     true,
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Ticks, 60)

	require.Equal(t, &protobuf.Tick{TickNumber: 81, IsEmpty: false}, response.Ticks[0])
	require.Equal(t, &protobuf.Tick{TickNumber: 22, IsEmpty: false}, response.Ticks[59])

	response, err = server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{
		Epoch:    123,
		Page:     4,
		PageSize: 60,
		Desc:     true,
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Ticks, 21)

	require.Equal(t, &protobuf.Tick{TickNumber: 21, IsEmpty: false}, response.Ticks[0])
	require.Equal(t, &protobuf.Tick{TickNumber: 1, IsEmpty: true}, response.Ticks[20])
}

func TestRpcServer_QueryEmptyTicks_GivenValidEpochValues_ThenNoError(t *testing.T) {
	tickIntervals := []*statusPb.TickInterval{{Epoch: 123, FirstTick: 1, LastTick: 100}}
	statusCache := NewStatusCache(nil, time.Minute, time.Minute)
	statusCache.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	qs := &QueryService{
		elasticClient: &FakeElasticClient{},
		cache:         statusCache,
	}

	server := Server{qb: qs, statusService: nil}

	_, err := server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 122})
	require.NoError(t, err)

	_, err = server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 123})
	require.NoError(t, err)
}

func TestRpcServer_QueryEmptyTicks_GivenEpochInThePast_ThenError(t *testing.T) {
	tickIntervals := []*statusPb.TickInterval{{Epoch: 123, FirstTick: 1, LastTick: 100}}
	statusCache := NewStatusCache(nil, time.Minute, time.Minute)
	statusCache.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	qs := &QueryService{
		elasticClient: &FakeElasticClient{},
		cache:         statusCache,
	}

	server := Server{qb: qs, statusService: nil}

	_, err := server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 121})
	require.Error(t, err)
	require.ErrorContains(t, err, "InvalidArgument")
	require.ErrorContains(t, err, "old")
}

func TestRpcServer_QueryEmptyTicks_GivenEpochInTheFuture_ThenError(t *testing.T) {
	tickIntervals := []*statusPb.TickInterval{{Epoch: 123, FirstTick: 1, LastTick: 100}}
	statusCache := NewStatusCache(nil, time.Minute, time.Minute)
	statusCache.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	qs := &QueryService{
		elasticClient: &FakeElasticClient{},
		cache:         statusCache,
	}

	server := Server{qb: qs, statusService: nil}

	_, err := server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 124})
	require.Error(t, err)
	require.ErrorContains(t, err, "InvalidArgument")
	require.ErrorContains(t, err, "future")
}

func TestRpcServer_QueryEmptyTicks_GivenInvalidPageSize_ThenError(t *testing.T) {
	tickIntervals := []*statusPb.TickInterval{{Epoch: 123, FirstTick: 1, LastTick: 100}}
	statusCache := NewStatusCache(nil, time.Minute, time.Minute)
	statusCache.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	qs := &QueryService{
		elasticClient: &FakeElasticClient{},
		cache:         statusCache,
	}

	server := Server{qb: qs, statusService: nil}

	_, err := server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 123, PageSize: 10000})
	require.Error(t, err)
	require.ErrorContains(t, err, "InvalidArgument")
	require.ErrorContains(t, err, "page size")

	_, err = server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 123, PageSize: 11})
	require.Error(t, err)
	require.ErrorContains(t, err, "InvalidArgument")
	require.ErrorContains(t, err, "page size")
}

func TestRpcServer_QueryEmptyTicks_GivenValidPageSize_ThenNoError(t *testing.T) {
	tickIntervals := []*statusPb.TickInterval{{Epoch: 123, FirstTick: 1, LastTick: 100}}
	statusCache := NewStatusCache(nil, time.Minute, time.Minute)
	statusCache.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	qs := &QueryService{
		elasticClient: &FakeElasticClient{},
		cache:         statusCache,
	}

	server := Server{qb: qs, statusService: nil}

	// special treatment for page size 1
	_, err := server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 123, PageSize: 1})
	require.NoError(t, err)

	_, err = server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 123, PageSize: 10})
	require.NoError(t, err)

	_, err = server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 123, PageSize: 120})
	require.NoError(t, err)

	_, err = server.GetEpochTickListV2(context.Background(), &protobuf.GetEpochTickListRequestV2{Epoch: 123, PageSize: 1000})
	require.NoError(t, err)
}
