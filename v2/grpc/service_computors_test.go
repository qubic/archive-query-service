package grpc

import (
	"context"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ComputorsServiceStub struct {
	computors []*api.ComputorsList
}

func (c *ComputorsServiceStub) GetComputorsListsForEpoch(_ context.Context, _ uint32) ([]*api.ComputorsList, error) {
	return c.computors, nil
}

func TestArchiverQueryService_GetComputorsList(t *testing.T) {
	expected := &api.TickData{TickNumber: 42}

	compsListService := &ComputorsServiceStub{
		computors: []*api.ComputorsList{{Identities: []string{"foo"}}},
	}
	service := NewArchiveQueryService(nil, nil, nil, compsListService)
	response, err := service.GetComputorsListsForEpoch(context.Background(), &api.GetComputorsListForEpochRequest{Epoch: 42})
	require.NoError(t, err)
	require.NotEmpty(t, expected, response.ComputorsLists)
}

func TestArchiverQueryService_GetComputorsList_GivenNoComputors_ThenReturnNotFound(t *testing.T) {
	compsListService := &ComputorsServiceStub{
		computors: []*api.ComputorsList{},
	}
	service := NewArchiveQueryService(nil, nil, nil, compsListService)
	_, err := service.GetComputorsListsForEpoch(context.Background(), &api.GetComputorsListForEpochRequest{Epoch: 666})
	assert.Error(t, err)
	require.Equal(t, status.Error(codes.NotFound, "computor lists not found"), err)
}
