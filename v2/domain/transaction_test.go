package domain

import (
	"context"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/domain/mock"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"testing"
)

func maxTickFetcherFunc(_ context.Context) (uint32, error) {
	return 10, nil
}

func TestTransactionService_GetTransactionByHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mock.NewMockTransactionRepository(ctrl)
	serv := NewTransactionService(repo, maxTickFetcherFunc)
	repo.EXPECT().GetTransactionByHash(gomock.Any(), gomock.Any()).Return(&api.Transaction{Source: "test"}, nil)

	tx, err := serv.GetTransactionByHash(context.Background(), "test-hash")
	require.NoError(t, err)
	diff := cmp.Diff(&api.Transaction{Source: "test"}, tx, cmpopts.IgnoreUnexported(api.Transaction{}))
	require.Empty(t, diff, "running test GetTransactionByHash")
}

func TestTransactionService_GetTransactionByIdentity(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mock.NewMockTransactionRepository(ctrl)
	service := NewTransactionService(repo, maxTickFetcherFunc)
	// []*api.Transaction, *entities.Hits, error
	apiTransactions := []*api.Transaction{{Hash: "test-hash-1"}, {Hash: "test-hash-2"}}
	entityHits := &entities.Hits{Total: 42, Relation: "eq"}
	ctx := context.Background()
	repo.EXPECT().GetTransactionsForIdentity(ctx, "test-identity", uint32(10), nil, nil, uint32(0), uint32(2)).Return(apiTransactions, entityHits, nil)

	result, err := service.GetTransactionsForIdentity(ctx, "test-identity", nil, nil, 0, 2)
	require.NoError(t, err)

	require.Len(t, result.GetTransactions(), 2)
	require.Equal(t, 10, int(result.LastProcessedTick))
	assert.Equal(t, apiTransactions, result.GetTransactions())
	assert.Equal(t, entityHits, result.GetHits())
}
