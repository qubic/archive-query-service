package grpc

import (
	"context"
	"fmt"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/test"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StatusServiceStub struct {
	statusResponse *statusPb.GetStatusResponse
	statusErr      error
}

func (s *StatusServiceStub) GetStatus(_ context.Context) (*statusPb.GetStatusResponse, error) {
	return s.statusResponse, s.statusErr
}

func (s *StatusServiceStub) GetProcessedTickIntervals(_ context.Context) ([]*api.ProcessedTickInterval, error) {
	return nil, nil
}

func defaultStatusStub() *StatusServiceStub {
	return &StatusServiceStub{
		statusResponse: &statusPb.GetStatusResponse{LastProcessedLogTick: 999999},
	}
}

type EventsServiceStub struct {
	events          []*api.Event
	hits            *entities.Hits
	err             error
	ReceivedFilters entities.Filters
}

const validId1 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB"
const validId2 = "BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID"
const validTransactionHash1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"
const validTransactionHash2 = "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaarmid"

func (s *EventsServiceStub) GetEvents(_ context.Context, queryFilters entities.Filters, _, _, _ uint32) (*entities.EventsResult, error) {
	s.ReceivedFilters = queryFilters
	if s.err != nil {
		return nil, s.err
	}
	return &entities.EventsResult{Hits: s.hits, Events: s.events}, nil
}

func TestArchiveQueryService_GetEvents_Success(t *testing.T) {
	evService := &EventsServiceStub{
		events: []*api.Event{
			{TickNumber: 100, TransactionHash: test.ToStringPointer(validTransactionHash1), LogType: 0, EventData: &api.Event_QuTransfer{
				QuTransfer: &api.QuTransferData{Source: "SRC", Destination: "DST", Amount: 1000},
			}},
			{TickNumber: 101, TransactionHash: test.ToStringPointer(validTransactionHash2), LogType: 1, EventData: &api.Event_AssetIssuance{
				AssetIssuance: &api.AssetIssuanceData{AssetIssuer: "ISSUER", AssetName: "QX"},
			}},
		},
		hits: &entities.Hits{Total: 2, Relation: "eq"},
	}
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, evService, NewPageSizeLimits(1000, 10))

	response, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
		Filters:    map[string]string{"transactionHash": validTransactionHash1},
		Pagination: &api.Pagination{Offset: 0, Size: 10},
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Len(t, response.EventLogs, 2)
	assert.Equal(t, uint32(2), response.Hits.Total)
	assert.Equal(t, uint32(0), response.Hits.From)
	assert.Equal(t, uint32(10), response.Hits.Size)
	assert.Equal(t, uint32(999999), response.ValidForTick)
}

func TestArchiveQueryService_GetEvents_InvalidFilter(t *testing.T) {
	evService := &EventsServiceStub{}
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
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
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
		Filters: map[string]string{"logType": "256"},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "invalid [logType] filter")
}

func TestArchiveQueryService_GetEvents_InvalidPagination(t *testing.T) {
	evService := &EventsServiceStub{}
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
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
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{})
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
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, evService, NewPageSizeLimits(1000, 10))

	response, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Empty(t, response.EventLogs)
	assert.Equal(t, uint32(0), response.Hits.Total)
}

func TestArchiveQueryService_GetEvents_GivenInvalidExcludeFilter_ThenError(t *testing.T) {
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, nil, NewPageSizeLimits(1000, 10))
	_, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
		Exclude: map[string]string{"tickNumber": "123"},
	})
	require.ErrorContains(t, err, "creating exclude filter")
	require.ErrorContains(t, err, "unsupported filter")
}

func TestArchiveQueryService_GetEvents_WithRanges(t *testing.T) {
	evService := &EventsServiceStub{
		events: []*api.Event{{}}, // single dummy event
		hits:   &entities.Hits{Total: 1, Relation: "eq"},
	}
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, evService, NewPageSizeLimits(1000, 10))

	response, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
		Ranges: map[string]*api.Range{
			"amount": {
				LowerBound: &api.Range_Gte{Gte: "1000"},
				UpperBound: &api.Range_Lte{Lte: "2000"},
			},
		},
		Pagination: &api.Pagination{Offset: 0, Size: 10},
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Len(t, response.EventLogs, 1)
	assert.Equal(t, uint32(1), response.Hits.Total)

	ranges := evService.ReceivedFilters.Ranges
	assert.Len(t, ranges, 1)
	assert.Len(t, ranges["amount"], 2)
	assert.Contains(t, ranges["amount"], entities.Range{Operation: "gte", Value: "1000"})
	assert.Contains(t, ranges["amount"], entities.Range{Operation: "lte", Value: "2000"})
}

func TestArchiveQueryService_GetEvents_WithShouldFilters(t *testing.T) {
	evService := &EventsServiceStub{
		events: []*api.Event{{}}, // single dummy event
		hits:   &entities.Hits{Total: 1, Relation: "eq"},
	}
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, evService, NewPageSizeLimits(1000, 10))

	response, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
		Should: []*api.ShouldFilter{
			{Terms: map[string]string{"destination": validId1 + " , " + validId2, "source": validId1}},
			{Ranges: map[string]*api.Range{
				"amount":         {LowerBound: &api.Range_Gte{Gte: "1000000"}, UpperBound: &api.Range_Lte{Lte: "2000000"}},
				"numberOfShares": {LowerBound: &api.Range_Gt{Gt: "0"}, UpperBound: &api.Range_Lt{Lt: "100"}},
			}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Len(t, response.EventLogs, 1)
	assert.Equal(t, uint32(1), response.Hits.Total)

	should := evService.ReceivedFilters.Should
	assert.Len(t, should, 2)
	assert.Len(t, should[0].Terms, 2)
	assert.Len(t, should[1].Ranges, 2)
	assert.Contains(t, should[0].Terms["destination"], validId1, validId2)
	assert.Contains(t, should[0].Terms["source"], validId1)
	assert.Contains(t, should[1].Ranges["amount"], entities.Range{Operation: "gte", Value: "1000000"}, entities.Range{Operation: "lte", Value: "2000000"})
	assert.Contains(t, should[1].Ranges["numberOfShares"], entities.Range{Operation: "gt", Value: "0"}, entities.Range{Operation: "lt", Value: "10"})
}

func TestArchiveQueryService_GetEvents_WithShouldFilterWithOnlyOneValue_ThenError(t *testing.T) {
	service := NewArchiveQueryService(nil, nil, defaultStatusStub(), nil, nil, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
		Should: []*api.ShouldFilter{
			{Terms: map[string]string{"destination": validId1 + " , " + validId2}},
		},
	})
	require.ErrorContains(t, err, "at least two")

}

func TestArchiveQueryService_GetEvents_TickNumberExceedsLastProcessed(t *testing.T) {
	statusStub := &StatusServiceStub{
		statusResponse: &statusPb.GetStatusResponse{LastProcessedLogTick: 50000},
	}
	evService := &EventsServiceStub{}
	service := NewArchiveQueryService(nil, nil, statusStub, nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
		Filters: map[string]string{"tickNumber": "60000"},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Contains(t, st.Message(), "greater than last processed tick")

	details := st.Details()
	require.Len(t, details, 1)
	lpt, ok := details[0].(*api.LastProcessedTick)
	require.True(t, ok)
	assert.Equal(t, uint32(50000), lpt.TickNumber)
}

func TestArchiveQueryService_GetEvents_TickNumberWithinRange(t *testing.T) {
	statusStub := &StatusServiceStub{
		statusResponse: &statusPb.GetStatusResponse{LastProcessedLogTick: 50000},
	}
	evService := &EventsServiceStub{
		events: []*api.Event{{}},
		hits:   &entities.Hits{Total: 1, Relation: "eq"},
	}
	service := NewArchiveQueryService(nil, nil, statusStub, nil, evService, NewPageSizeLimits(1000, 10))

	response, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{
		Filters: map[string]string{"tickNumber": "40000"},
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Equal(t, uint32(50000), response.ValidForTick)
}

func TestArchiveQueryService_GetEvents_StatusServiceError(t *testing.T) {
	statusStub := &StatusServiceStub{
		statusErr: fmt.Errorf("status service unavailable"),
	}
	evService := &EventsServiceStub{}
	service := NewArchiveQueryService(nil, nil, statusStub, nil, evService, NewPageSizeLimits(1000, 10))

	_, err := service.GetEvents(context.Background(), &api.GetEventLogsRequest{})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
}
