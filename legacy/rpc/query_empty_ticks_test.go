package rpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/qubic/archive-query-service/legacy/elastic"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const key = "%d-%d-%d"

type FakeElasticClient struct {
	emptyTicks           map[string][]uint32
	identityTransactions elastic.TransactionsSearchResponse
	err                  error
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
	return f.identityTransactions, f.err
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
			emptyTicks: map[string][]uint32{"123-1-10": {1, 2, 3}, "123-100-200": {101, 102, 200}},
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
			LastTick:  203, // plus query offset
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

func TestCalculateRange(t *testing.T) {
	tests := []struct {
		name     string
		endTick  uint32
		interval *statusPb.TickInterval
		index    int
		length   int
		wantFrom uint32
		wantTo   uint32
	}{
		{
			name:     "not last interval: returns full interval range",
			endTick:  10,
			interval: &statusPb.TickInterval{FirstTick: 5, LastTick: 20},
			index:    0,
			length:   2,
			wantFrom: 11, // max(10+1, 5)
			wantTo:   20,
		},
		{
			name:     "last interval, gap <= offset: no offset applied",
			endTick:  17,
			interval: &statusPb.TickInterval{FirstTick: 5, LastTick: 20},
			index:    0,
			length:   1,
			// anotherCall: 17 < 20-3=17 → false, so to = LastTick
			wantFrom: 18, // max(17+1, 5)
			wantTo:   20,
		},
		{
			name:     "last interval, gap > offset: offset applied",
			endTick:  10,
			interval: &statusPb.TickInterval{FirstTick: 5, LastTick: 20},
			index:    0,
			length:   1,
			// anotherCall: 10 < 20-3=17 → true, to = max(11, 17) = 17
			wantFrom: 11, // max(10+1, 5)
			wantTo:   17, // LastTick - emptyTickQueryOffset
		},
		{
			name:     "last interval, from > interval.FirstTick: from uses EndTick+1",
			endTick:  15,
			interval: &statusPb.TickInterval{FirstTick: 5, LastTick: 20},
			index:    0,
			length:   1,
			// anotherCall: 15 < 17 → true, to = max(16, 17) = 17
			wantFrom: 16, // max(15+1, 5)
			wantTo:   17,
		},
		{
			name:     "last interval, from == LastTick-offset: to = from", // repeated call
			endTick:  17,
			interval: &statusPb.TickInterval{FirstTick: 5, LastTick: 20},
			index:    0,
			length:   1,
			// anotherCall: 17 < 17 → false, so to = LastTick
			wantFrom: 18,
			wantTo:   20,
		},
		{
			name:     "last interval, from > LastTick-offset: to = from",
			endTick:  18,
			interval: &statusPb.TickInterval{FirstTick: 5, LastTick: 20},
			index:    0,
			length:   1,
			// anotherCall: 18 < 17 → false, so to = LastTick
			wantFrom: 19,
			wantTo:   20,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			emptyTicks := &EmptyTicks{EndTick: tc.endTick}
			from, to := calculateRange(emptyTicks, tc.interval, tc.index, tc.length)
			assert.Equal(t, tc.wantFrom, from)
			assert.Equal(t, tc.wantTo, to)
		})
	}
}
