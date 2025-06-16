package domain

import (
	"context"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/domain/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"testing"
)

func maxTickFetcherFunc(ctx context.Context) (uint32, error) {
	return 10, nil
}

func TestTransactionService_GetTransactionByHash(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repo := mock.NewMockTransactionRepository(ctrl)

	serv := NewTransactionService(maxTickFetcherFunc, repo)
	repo.EXPECT().GetTransactionByHash(gomock.Any(), gomock.Any()).Return(&api.Transaction{Source: "test"}, nil)

	tx, err := serv.GetTransactionByHash(context.Background(), "test-hash")
	require.NoError(t, err)
	diff := cmp.Diff(&api.Transaction{Source: "test"}, tx, cmpopts.IgnoreUnexported(api.Transaction{}))
	require.Empty(t, diff, "running test GetTransactionByHash")
}
