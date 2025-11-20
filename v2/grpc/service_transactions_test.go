package grpc

import (
	"context"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TransactionServiceStub struct {
	ctx          context.Context
	identity     string
	filters      map[string]string
	ranges       map[string][]*entities.Range
	transactions []*api.Transaction
	hits         *entities.Hits
}

func (t *TransactionServiceStub) GetTransactionByHash(_ context.Context, hash string) (*api.Transaction, error) {
	for _, tx := range t.transactions {
		if tx.Hash == hash {
			return tx, nil
		}
	}
	return nil, nil
}

func (t *TransactionServiceStub) GetTransactionsForTickNumber(_ context.Context, tickNumber uint32) ([]*api.Transaction, error) {
	transactions := make([]*api.Transaction, 0)
	for _, tx := range t.transactions {
		if tx.TickNumber == tickNumber {
			transactions = append(transactions, tx)
		}
	}
	return transactions, nil
}

func (t *TransactionServiceStub) GetTransactionsForIdentity(
	ctx context.Context,
	identity string,
	filters map[string]string,
	ranges map[string][]*entities.Range,
	_, _ uint32,
) (*entities.TransactionsResult, error) {
	t.ctx = ctx
	t.identity = identity
	t.filters = filters
	t.ranges = ranges
	return &entities.TransactionsResult{LastProcessedTick: 42, Hits: t.hits, Transactions: t.transactions}, nil
}

func TestArchiverQueryService_GetTransactionByHash(t *testing.T) {
	expected := &api.Transaction{Hash: "tx-hash"}

	txService := &TransactionServiceStub{
		transactions: []*api.Transaction{expected},
	}
	service := NewArchiveQueryService(txService, nil, nil, nil, PaginationLimits{maxHitsSize: 10000})
	response, err := service.GetTransactionByHash(context.Background(), &api.GetTransactionByHashRequest{Hash: "tx-hash"})
	require.NoError(t, err)
	require.Equal(t, expected, response.Transaction)

}

func TestArchiverQueryService_GetTransactionByHash_GivenNoTransaction_ThenReturnNotFound(t *testing.T) {
	txService := &TransactionServiceStub{
		transactions: []*api.Transaction{},
	}
	service := NewArchiveQueryService(txService, nil, nil, nil, PaginationLimits{maxHitsSize: 10000})
	_, err := service.GetTransactionByHash(context.Background(), &api.GetTransactionByHashRequest{Hash: "not-found"})
	require.Error(t, err)
	require.Equal(t, status.Error(codes.NotFound, "transaction not found"), err)
}

func TestArchiverQueryService_GetTransactionsForTick(t *testing.T) {
	txService := &TransactionServiceStub{
		transactions: []*api.Transaction{{Hash: "tx-hash-1", TickNumber: 42}, {Hash: "tx-hash-2", TickNumber: 43}},
	}
	service := NewArchiveQueryService(txService, nil, nil, nil, PaginationLimits{maxHitsSize: 10000})
	response, err := service.GetTransactionsForTick(context.Background(), &api.GetTransactionsForTickRequest{TickNumber: 42})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Contains(t, response.Transactions, txService.transactions[0])
}

func TestArchiverQueryService_GetTransactionsForTick_GivenNoTransaction_ThenReturnEmptyList(t *testing.T) {
	txService := &TransactionServiceStub{
		transactions: []*api.Transaction{{Hash: "tx-hash-1", TickNumber: 42}, {Hash: "tx-hash-2", TickNumber: 43}},
	}
	service := NewArchiveQueryService(txService, nil, nil, nil, PaginationLimits{maxHitsSize: 10000})
	response, err := service.GetTransactionsForTick(context.Background(), &api.GetTransactionsForTickRequest{TickNumber: 666})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Empty(t, response.Transactions)
}

func TestArchiveQueryService_GetTransactionsForIdentity(t *testing.T) {
	txService := &TransactionServiceStub{
		transactions: []*api.Transaction{{Hash: "tx-hash-1"}, {Hash: "tx-hash-2"}},
		hits:         &entities.Hits{Total: 2, Relation: "eq"},
	}

	service := NewArchiveQueryService(txService, nil, nil, nil, PaginationLimits{maxHitsSize: 10000})

	from := uint32(0)
	size := uint32(10)

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
		Pagination: &api.Pagination{Offset: from, Size: size},
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
