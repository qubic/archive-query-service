package test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	rpc "github.com/qubic/archive-query-service/v2/grpc"
	"github.com/qubic/archive-query-service/v2/grpc/mock"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestServer(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}

type ServerTestSuite struct {
	suite.Suite
	client            api.ArchiveQueryServiceClient
	grpcClientConn    *grpc.ClientConn
	srv               *rpc.ArchiveQueryService
	mockCtrl          *gomock.Controller
	mockTxService     *mock.MockTransactionsService
	mockStatusService *mock.MockStatusService
	mockEvService     *mock.MockEventsService
}

func (s *ServerTestSuite) SetupSuite() {
	t := s.T()

	ctrl := gomock.NewController(t)

	srvErrorsChan := make(chan error, 1)

	mockTxService := mock.NewMockTransactionsService(ctrl)
	mockStatusService := mock.NewMockStatusService(ctrl)
	mockEvService := mock.NewMockEventsService(ctrl)
	rpcServer := rpc.NewArchiveQueryService(mockTxService, nil, mockStatusService, nil, mockEvService, rpc.NewPageSizeLimits(1000, 10))
	tickInBoundsInterceptor := rpc.NewTickWithinBoundsInterceptor(mockStatusService)
	var identitiesValidatorInterceptor rpc.IdentitiesValidatorInterceptor
	var logTechnicalErrorInterceptor rpc.LogTechnicalErrorInterceptor
	startCfg := rpc.StartConfig{
		ListenAddrGRPC: "127.0.0.1:0", // Use a random port for testing
		MaxRecvMsgSize: 1 * 1024 * 1024,
		MaxSendMsgSize: 1 * 1024 * 1024,
	}

	err := rpcServer.Start(startCfg, srvErrorsChan,
		logTechnicalErrorInterceptor.GetInterceptor,
		tickInBoundsInterceptor.GetInterceptor,
		identitiesValidatorInterceptor.GetInterceptor)
	require.NoError(t, err, "starting grpc server")

	conn, err := grpc.NewClient(rpcServer.GetGRPCListenAddr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "creating grpc client")

	client := api.NewArchiveQueryServiceClient(conn)

	s.client = client
	s.grpcClientConn = conn
	s.srv = rpcServer
	s.mockCtrl = ctrl
	s.mockTxService = mockTxService
	s.mockStatusService = mockStatusService
	s.mockEvService = mockEvService
}

func (s *ServerTestSuite) TearDownSuite() {
	if s.mockCtrl != nil {
		s.mockCtrl.Finish()
	}
	if s.srv != nil {
		s.srv.Stop()
	}
	if s.grpcClientConn != nil {
		s.grpcClientConn.Close()
	}
}

func (s *ServerTestSuite) TestGetLastProcessedTick() {
	t := s.T()
	s.mockStatusService.EXPECT().GetStatus(gomock.Any()).Return(&statusPb.GetStatusResponse{
		LastProcessedTick:   125,
		ProcessingEpoch:     100,
		IntervalInitialTick: 10,
	}, nil)
	resp, err := s.client.GetLastProcessedTick(t.Context(), nil)
	require.NoError(t, err, "getting last processed tick")

	expected := &api.GetLastProcessedTickResponse{
		TickNumber:          125,
		Epoch:               100,
		IntervalInitialTick: 10,
	}
	diff := cmp.Diff(expected, resp, protocmp.Transform())
	require.Empty(t, diff, "expected last processed tick to match")
}

func (s *ServerTestSuite) TestGetTickIntervals() {
	t := s.T()
	s.mockStatusService.EXPECT().GetProcessedTickIntervals(gomock.Any()).Return([]*api.ProcessedTickInterval{
		{
			Epoch:     1,
			FirstTick: 100,
			LastTick:  200,
		},
		{
			Epoch:     2,
			FirstTick: 201,
			LastTick:  300,
		},
	}, nil)
	resp, err := s.client.GetProcessedTickIntervals(t.Context(), nil)
	require.NoError(t, err, "getting processed tick intervals")

	expected := &api.GetProcessedTickIntervalsResponse{
		ProcessedTickIntervals: []*api.ProcessedTickInterval{
			{
				Epoch:     1,
				FirstTick: 100,
				LastTick:  200,
			},
			{
				Epoch:     2,
				FirstTick: 201,
				LastTick:  300,
			},
		},
	}
	diff := cmp.Diff(expected, resp, protocmp.Transform())
	require.Empty(t, diff, "expected processed tick intervals to match")
}

func (s *ServerTestSuite) TestGetEvents_Success() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), uint32(0), uint32(10)).
		Return(&entities.EventsResult{
			Hits: &entities.Hits{Total: 2, Relation: "eq"},
			Events: []*api.Event{
				{TickNumber: 100, TransactionHash: ToPointer("hash1"), EventType: 0, EventData: &api.Event_QuTransfer{
					QuTransfer: &api.QuTransferData{Source: "SRC", Destination: "DST", Amount: 1000},
				}},
				{TickNumber: 101, TransactionHash: ToPointer("hash2"), EventType: 1, EventData: &api.Event_AssetIssuance{
					AssetIssuance: &api.AssetIssuanceData{AssetIssuer: "ISSUER", AssetName: "QX"},
				}},
			},
		}, nil)

	resp, err := s.client.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters:    map[string]string{"transactionHash": "hash1"},
		Pagination: &api.Pagination{Offset: 0, Size: 10},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.Events, 2)
	assert.Equal(t, uint32(2), resp.Hits.Total)
	assert.Equal(t, uint32(0), resp.Hits.From)
	assert.Equal(t, uint32(10), resp.Hits.Size)

	// Verify oneof fields
	assert.NotNil(t, resp.Events[0].GetQuTransfer())
	assert.Equal(t, "SRC", resp.Events[0].GetQuTransfer().GetSource())
	assert.NotNil(t, resp.Events[1].GetAssetIssuance())
	assert.Equal(t, "ISSUER", resp.Events[1].GetAssetIssuance().GetAssetIssuer())
}

func (s *ServerTestSuite) TestGetEvents_FilterByTransactionHash() {
	t := s.T()
	expectedFilters := map[string][]string{"transactionHash": {"txhash1"}}
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), expectedFilters, uint32(0), uint32(10)).
		Return(&entities.EventsResult{
			Hits:   &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{TickNumber: 100, TransactionHash: ToPointer("txhash1"), EventType: 0}},
		}, nil)

	resp, err := s.client.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"transactionHash": "txhash1"},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 1)
	assert.Equal(t, "txhash1", *resp.Events[0].TransactionHash)
}

func (s *ServerTestSuite) TestGetEvents_FilterByTickNumber() {
	t := s.T()
	expectedFilters := map[string][]string{"tickNumber": {"15001"}}
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), expectedFilters, uint32(0), uint32(10)).
		Return(&entities.EventsResult{
			Hits:   &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{TickNumber: 15001, EventType: 0}},
		}, nil)

	resp, err := s.client.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"tickNumber": "15001"},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 1)
	assert.Equal(t, uint32(15001), resp.Events[0].TickNumber)
}

func (s *ServerTestSuite) TestGetEvents_FilterByEventType() {
	t := s.T()
	expectedFilters := map[string][]string{"eventType": {"8"}}
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), expectedFilters, uint32(0), uint32(10)).
		Return(&entities.EventsResult{
			Hits:   &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{TickNumber: 200, EventType: 8}},
		}, nil)

	resp, err := s.client.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"eventType": "8"},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 1)
	assert.Equal(t, uint32(8), resp.Events[0].EventType)
}

func (s *ServerTestSuite) TestGetEvents_CombinedFilters() {
	t := s.T()
	expectedFilters := map[string][]string{
		"transactionHash": {"txhash1"},
		"eventType":       {"0"},
	}
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), expectedFilters, uint32(0), uint32(10)).
		Return(&entities.EventsResult{
			Hits:   &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{TickNumber: 100, TransactionHash: ToPointer("txhash1"), EventType: 0}},
		}, nil)

	resp, err := s.client.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"transactionHash": "txhash1", "eventType": "0"},
	})
	require.NoError(t, err)
	assert.Len(t, resp.Events, 1)
}

func (s *ServerTestSuite) TestGetEvents_InvalidFilter() {
	t := s.T()
	_, err := s.client.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"unsupported": "value"},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "unsupported filter")
}

func (s *ServerTestSuite) TestGetEvents_InvalidEventType() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	_, err := s.client.GetEvents(ctx, &api.GetEventsRequest{
		Filters: map[string]string{"eventType": "invalid"},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "validating filters")
	assert.Contains(t, st.Message(), "invalid [eventType] filter")
}

func (s *ServerTestSuite) TestGetEvents_InvalidTickNumber() {
	t := s.T()
	_, err := s.client.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"tickNumber": "not-a-number"},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func (s *ServerTestSuite) TestGetEvents_Pagination() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), uint32(5), uint32(3)).
		Return(&entities.EventsResult{
			Hits:   &entities.Hits{Total: 20, Relation: "eq"},
			Events: []*api.Event{{}, {}, {}},
		}, nil)

	resp, err := s.client.GetEvents(t.Context(), &api.GetEventsRequest{
		Pagination: &api.Pagination{Offset: 5, Size: 3},
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(5), resp.Hits.From)
	assert.Equal(t, uint32(3), resp.Hits.Size)
	assert.Equal(t, uint32(20), resp.Hits.Total)
}

func (s *ServerTestSuite) TestGetEvents_EmptyResult() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&entities.EventsResult{
			Hits:   &entities.Hits{Total: 0, Relation: "eq"},
			Events: []*api.Event{},
		}, nil)

	resp, err := s.client.GetEvents(t.Context(), &api.GetEventsRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.Events)
	assert.Equal(t, uint32(0), resp.Hits.Total)
}
