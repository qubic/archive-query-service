package domain

import (
	"context"
	"fmt"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/domain/mock"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestEventsService_GetEvents_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockRepo := mock.NewMockEventsRepository(ctrl)
	service := NewEventsService(mockRepo)

	expectedEvents := []*api.Event{
		{TickNumber: 100, TransactionHash: test.ToPointer("hash1"), EventType: 0, EventData: &api.Event_QuTransfer{
			QuTransfer: &api.QuTransferData{Source: "SRC", Destination: "DST", Amount: 1000},
		}},
		{TickNumber: 101, TransactionHash: test.ToPointer("hash2"), EventType: 1, EventData: &api.Event_AssetIssuance{
			AssetIssuance: &api.AssetIssuanceData{AssetIssuer: "ISSUER", AssetName: "QX"},
		}},
	}
	expectedHits := &entities.Hits{Total: 2, Relation: "eq"}

	filters := map[string][]string{"transactionHash": {"hash1"}}
	mockRepo.EXPECT().GetEvents(gomock.Any(), filters, uint32(0), uint32(10)).
		Return(expectedEvents, expectedHits, nil)

	result, err := service.GetEvents(context.Background(), filters, 0, 10)
	require.NoError(t, err)
	assert.Equal(t, expectedHits, result.Hits)
	assert.Equal(t, expectedEvents, result.Events)
}

func TestEventsService_GetEvents_RepoError(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockRepo := mock.NewMockEventsRepository(ctrl)
	service := NewEventsService(mockRepo)

	mockRepo.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, fmt.Errorf("connection refused"))

	result, err := service.GetEvents(context.Background(), nil, 0, 10)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "connection refused")
}

func TestEventsService_GetEvents_EmptyResult(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockRepo := mock.NewMockEventsRepository(ctrl)
	service := NewEventsService(mockRepo)

	mockRepo.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]*api.Event{}, &entities.Hits{Total: 0, Relation: "eq"}, nil)

	result, err := service.GetEvents(context.Background(), nil, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, result.Events)
	assert.Equal(t, 0, result.Hits.Total)
}
