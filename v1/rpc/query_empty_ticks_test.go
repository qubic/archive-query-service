package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/qubic/archive-query-service/elastic"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const key = "%d-%d-%d"

type FakeElasticClient struct {
	emptyTicks map[string][]uint32
}

func (f *FakeElasticClient) QueryTickDataByTickNumber(context.Context, uint32) (elastic.TickDataGetResponse, error) {
	panic("implement me")
}

func (f *FakeElasticClient) QueryEmptyTicks(_ context.Context, startTick, endTick, epoch uint32) ([]uint32, error) {
	ticks, _ := f.emptyTicks[fmt.Sprintf(key, epoch, startTick, endTick)]
	return ticks, nil
}

func (f *FakeElasticClient) QueryComputorListByEpoch(context.Context, uint32) (elastic.ComputorsListSearchResponse, error) {
	panic("implement me")
}

func (f *FakeElasticClient) QueryIdentityTransactions(_ context.Context, _ string, _, _ int, _ bool, _, _ uint32) (elastic.TransactionsSearchResponse, error) {
	panic("implement me")
}

func (f *FakeElasticClient) QueryTransactionByHash(context.Context, string) (elastic.TransactionGetResponse, error) {
	panic("implement me")
}

func (f *FakeElasticClient) QueryTickTransactions(context.Context, uint32) (elastic.TransactionsSearchResponse, error) {
	panic("implement me")
}

func TestQueryService_GetEmptyTicks_ThenCreateAndCache(t *testing.T) {
	qs := &QueryService{
		elasticClient: &FakeElasticClient{
			emptyTicks: map[string][]uint32{"123-1-10": {1, 2, 3}},
		},
		cache: NewStatusCache(nil, time.Second, time.Second),
	}

	intervals := []*statusPb.TickInterval{
		{
			Epoch:     123,
			FirstTick: 1,
			LastTick:  10,
		},
	}

	emptyTicks, err := qs.GetEmptyTicks(context.Background(), 123, intervals)
	require.NoError(t, err)
	require.NotNil(t, emptyTicks)

	assert.Equal(t, &EmptyTicks{
		Epoch:     123,
		StartTick: 1,
		EndTick:   10,
		Ticks:     map[uint32]bool{1: true, 2: true, 3: true},
	}, emptyTicks)

	cached := qs.cache.GetEmptyTicks(123)
	assert.Equal(t, emptyTicks, cached)
}

func TestQueryService_GetEmptyTicks_GivenNewIntervalEnd_ThenUpdate(t *testing.T) {
	qs := &QueryService{
		elasticClient: &FakeElasticClient{
			emptyTicks: map[string][]uint32{"123-11-11": {1, 2, 3, 11}}, // we query range 11-11
		},
		cache: NewStatusCache(nil, time.Second, time.Second),
	}

	// initially cached empty ticks
	qs.cache.SetEmptyTicks(&EmptyTicks{
		Epoch:     123,
		StartTick: 1,
		EndTick:   10,
		Ticks:     map[uint32]bool{1: true, 2: true, 3: true},
	})

	// new end tick 11
	intervals := []*statusPb.TickInterval{
		{
			Epoch:     123,
			FirstTick: 1,
			LastTick:  11,
		},
	}

	emptyTicks, err := qs.GetEmptyTicks(context.Background(), 123, intervals)
	require.NoError(t, err)
	require.NotNil(t, emptyTicks)

	updated := &EmptyTicks{
		Epoch:     123,
		StartTick: 1,
		EndTick:   11,
		Ticks:     map[uint32]bool{1: true, 2: true, 3: true, 11: true},
	}
	require.Equal(t, updated, emptyTicks)

	cached := qs.cache.GetEmptyTicks(123)
	require.Equal(t, emptyTicks, cached)
}

func TestQueryService_GetEmptyTicks_GivenNewInterval_ThenUpdate(t *testing.T) {
	qs := &QueryService{
		elasticClient: &FakeElasticClient{
			emptyTicks: map[string][]uint32{"123-1-10": {1, 2, 3}, "123-100-200": {101, 102, 200}}, // we query range 11-11
		},
		cache: NewStatusCache(nil, time.Second, time.Second),
	}

	intervals := []*statusPb.TickInterval{
		{
			Epoch:     123,
			FirstTick: 1,
			LastTick:  10,
		},
	}

	emptyTicks, err := qs.GetEmptyTicks(context.Background(), 123, intervals)
	require.NoError(t, err)
	require.NotNil(t, emptyTicks)

	assert.Equal(t, &EmptyTicks{
		Epoch:     123,
		StartTick: 1,
		EndTick:   10,
		Ticks:     map[uint32]bool{1: true, 2: true, 3: true},
	}, emptyTicks)

	// new run with new interval
	intervals = []*statusPb.TickInterval{
		{
			Epoch:     123,
			FirstTick: 1,
			LastTick:  10,
		},
		{
			Epoch:     123,
			FirstTick: 100,
			LastTick:  200,
		},
	}

	emptyTicks, err = qs.GetEmptyTicks(context.Background(), 123, intervals)
	require.NoError(t, err)
	require.NotNil(t, emptyTicks)

	assert.Equal(t, &EmptyTicks{
		Epoch:     123,
		StartTick: 1,
		EndTick:   200,
		Ticks:     map[uint32]bool{1: true, 2: true, 3: true, 101: true, 102: true, 200: true},
	}, emptyTicks)

}

func TestQueryService_GetEmptyTicks_GivenMultipleIntervals_ThenQueryMultipleTimes(t *testing.T) {
	qs := &QueryService{
		elasticClient: &FakeElasticClient{
			emptyTicks: map[string][]uint32{"123-1-10": {1, 2, 3}, "123-100-200": {101, 102, 199, 200}},
		},
		cache: NewStatusCache(nil, time.Second, time.Second),
	}

	intervals := []*statusPb.TickInterval{
		{
			Epoch:     123,
			FirstTick: 1,
			LastTick:  10,
		},
		{
			Epoch:     123,
			FirstTick: 100,
			LastTick:  200,
		},
	}

	emptyTicks, err := qs.GetEmptyTicks(context.Background(), 123, intervals)
	require.NoError(t, err)
	require.NotNil(t, emptyTicks)

	assert.Equal(t, &EmptyTicks{
		Epoch:     123,
		StartTick: 1,
		EndTick:   200,
		Ticks:     map[uint32]bool{1: true, 2: true, 3: true, 101: true, 102: true, 199: true, 200: true},
	}, emptyTicks)

}
