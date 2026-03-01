package grpc

import (
	"context"
	"fmt"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EventsServiceStub struct {
	events []*api.Event
	hits   *entities.Hits
	err    error
}

func (s *EventsServiceStub) GetEvents(_ context.Context, _ map[string][]string, _, _ uint32) (*entities.EventsResult, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &entities.EventsResult{Hits: s.hits, Events: s.events}, nil
}

func TestArchiveQueryService_GetEvents_Success(t *testing.T) {
	evService := &EventsServiceStub{
		events: []*api.Event{
			{TickNumber: 100, TransactionHash: test.ToPointer("hash1"), EventType: 0, EventData: &api.Event_QuTransfer{
				QuTransfer: &api.QuTransferData{Source: "SRC", Destination: "DST", Amount: 1000},
			}},
			{TickNumber: 101, TransactionHash: test.ToPointer("hash2"), EventType: 1, EventData: &api.Event_AssetIssuance{
				AssetIssuance: &api.AssetIssuanceData{AssetIssuer: "ISSUER", AssetName: "QX"},
			}},
		},
		hits: &entities.Hits{Total: 2, Relation: "eq"},
	}
	service := NewArchiveQueryService(nil, nil, nil, nil, evService, NewPageSizeLimits(1000, 10))

	response, err := service.GetEvents(context.Background(), &api.GetEventsRequest{
		Filters:    map[string]string{"transactionHash": "hash1"},
		Pagination: &api.Pagination{Offset: 0, Size: 10},
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Len(t, response.Events, 2)
	assert.Equal(t, uint32(2), response.Hits.Total)
	assert.Equal(t, uint32(0), response.Hits.From)
	assert.Equal(t, uint32(10), response.Hits.Size)
}

func TestArchiveQueryService_GetEvents_InvalidFilter(t *testing.T) {
	evService := &EventsServiceStub{}
	service := NewArchiveQueryService(nil, nil, nil, nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventsRequest{
		Filters: map[string]string{"unsupported": "value"},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "unsupported filter")
}

func TestArchiveQueryService_GetEvents_InvalidEventType(t *testing.T) {
	evService := &EventsServiceStub{}
	service := NewArchiveQueryService(nil, nil, nil, nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventsRequest{
		Filters: map[string]string{"eventType": "256"},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "invalid [eventType] filter")
}

func TestArchiveQueryService_GetEvents_InvalidPagination(t *testing.T) {
	evService := &EventsServiceStub{}
	service := NewArchiveQueryService(nil, nil, nil, nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventsRequest{
		Pagination: &api.Pagination{Offset: 0, Size: 5000},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "invalid pagination")
}

func TestArchiveQueryService_GetEvents_ServiceError(t *testing.T) {
	evService := &EventsServiceStub{
		err: fmt.Errorf("elasticsearch unavailable"),
	}
	service := NewArchiveQueryService(nil, nil, nil, nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventsRequest{})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
}

func TestArchiveQueryService_GetEvents_EmptyResult(t *testing.T) {
	evService := &EventsServiceStub{
		events: []*api.Event{},
		hits:   &entities.Hits{Total: 0},
	}
	service := NewArchiveQueryService(nil, nil, nil, nil, evService, NewPageSizeLimits(1000, 10))

	response, err := service.GetEvents(context.Background(), &api.GetEventsRequest{})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Empty(t, response.Events)
	assert.Equal(t, uint32(0), response.Hits.Total)
}
