package grpc

import (
	"context"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type TransactionServiceStub struct {
	ctx          context.Context
	identity     string
	filters      map[string]string
	ranges       map[string][]*entities.Range
	transactions []*api.Transaction
	hits         *entities.Hits
}

func (t *TransactionServiceStub) GetTransactionByHash(context.Context, string) (*api.Transaction, error) {
	panic("implement me")
}

func (t *TransactionServiceStub) GetTransactionsForTickNumber(context.Context, uint32) ([]*api.Transaction, error) {
	panic("implement me")
}

func (t *TransactionServiceStub) GetTransactionsForIdentity(ctx context.Context, identity string, filters map[string]string, ranges map[string][]*entities.Range, _, _ uint32) (*TransactionsResult, error) {
	t.ctx = ctx
	t.identity = identity
	t.filters = filters
	t.ranges = ranges
	return &TransactionsResult{42, t.hits, t.transactions}, nil
}

func TestArchiveQueryService_GetTransactionsForIdentity(t *testing.T) {
	txService := &TransactionServiceStub{
		transactions: []*api.Transaction{{Hash: "tx-hash-1"}, {Hash: "tx-hash-2"}},
		hits:         &entities.Hits{Total: 2, Relation: "eq"},
	}

	service := NewArchiveQueryService(txService, nil, nil)

	var from uint32 = 0
	var size uint32 = 10

	ctx := context.Background()
	request := &api.GetTransactionsForIdentityRequest{
		Identity: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
		Filters:  map[string]string{"inputType": "1"},
		Ranges: map[string]*api.Range{
			"amount": {
				LowerBound: &api.Range_Gte{Gte: "1"},
				UpperBound: &api.Range_Lt{Lt: "10000"},
			},
		},
		Pagination: &api.Pagination{Offset: &from, Size: &size},
	}

	response, err := service.GetTransactionsForIdentity(ctx, request)
	require.NoError(t, err)

	// verify response
	assert.Equal(t, 2, len(response.Transactions))
	assert.Equal(t, "tx-hash-1", response.Transactions[0].Hash)
	assert.Equal(t, "tx-hash-2", response.Transactions[1].Hash)

	assert.Equal(t, 2, int(response.GetHits().GetTotal()))
	assert.Equal(t, 0, int(response.GetHits().GetFrom()))
	assert.Equal(t, 10, int(response.GetHits().GetSize()))

	assert.Equal(t, 42, int(response.GetValidForTick()))

	// verify tx service call
	assert.Equal(t, ctx, txService.ctx)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB", txService.identity)
	assert.Equal(t, request.GetFilters(), txService.filters)
	assert.Equal(t, map[string][]*entities.Range{"amount": {
		&entities.Range{Operation: "gte", Value: "1"},
		&entities.Range{Operation: "lt", Value: "10000"},
	}}, txService.ranges)
}
