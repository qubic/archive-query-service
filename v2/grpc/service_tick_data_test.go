package grpc

import (
	"context"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
)

type TickDataServiceStub struct {
	tickData *api.TickData
}

func (t *TickDataServiceStub) GetTickData(_ context.Context, tickNumber uint32) (*api.TickData, error) {
	if t.tickData.TickNumber == tickNumber {
		return t.tickData, nil
	}
	return nil, nil
}

func TestArchiverQueryService_GetTickData(t *testing.T) {
	expected := &api.TickData{TickNumber: 42}

	tdService := &TickDataServiceStub{
		tickData: expected,
	}
	service := NewArchiveQueryService(nil, tdService, nil, nil, NewPageSizeLimits(1000, 10))
	response, err := service.GetTickData(context.Background(), &api.GetTickDataRequest{TickNumber: 42})
	require.NoError(t, err)
	require.Equal(t, expected, response.TickData)
}

func TestArchiverQueryService_GetTickData_GivenNoTickData_ThenReturnEmptyTickData(t *testing.T) {
	expected := &api.TickData{TickNumber: 42}

	tdService := &TickDataServiceStub{
		tickData: expected,
	}
	service := NewArchiveQueryService(nil, tdService, nil, nil, NewPageSizeLimits(1000, 10))
	response, err := service.GetTickData(context.Background(), &api.GetTickDataRequest{TickNumber: 666})
	require.NoError(t, err)
	require.Nil(t, response.TickData)
}
