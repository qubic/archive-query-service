package test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	rpc "github.com/qubic/archive-query-service/v2/grpc"
	"github.com/qubic/archive-query-service/v2/grpc/mock"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/stretchr/testify/suite"
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
}

func (s *ServerTestSuite) SetupSuite() {
	t := s.T()

	ctrl := gomock.NewController(t)

	srvErrorsChan := make(chan error, 1)

	mockTxService := mock.NewMockTransactionsService(ctrl)
	mockStatusService := mock.NewMockStatusService(ctrl)
	rpcServer := rpc.NewArchiveQueryService(mockTxService, nil, mockStatusService, nil)
	tickInBoundsInterceptor := rpc.NewTickWithinBoundsInterceptor(mockStatusService)
	var identitiesValidatorInterceptor rpc.IdentitiesValidatorInterceptor
	var logTechnicalErrorInterceptor rpc.LogTechnicalErrorInterceptor
	startCfg := rpc.StartConfig{
		ListenAddrGRPC: "127.0.0.1:0", // Use a random port for testing
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

	expected := &api.GetProcessedTicksIntervalsResponse{
		ProcessedTicksIntervals: []*api.ProcessedTickInterval{
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
