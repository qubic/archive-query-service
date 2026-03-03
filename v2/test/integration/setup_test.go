package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/domain"
	"github.com/qubic/archive-query-service/v2/domain/repository/elastic"
	rpc "github.com/qubic/archive-query-service/v2/grpc"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcelastic "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

// --- Suite ---

type EventsE2ESuite struct {
	suite.Suite
	ctx            context.Context
	esContainer    testcontainers.Container
	srv            *rpc.ArchiveQueryService
	grpcClientConn *grpc.ClientConn
	grpcClient     api.ArchiveQueryServiceClient
	httpServer     *httptest.Server
}

func TestEventsE2E(t *testing.T) {
	suite.Run(t, new(EventsE2ESuite))
}

func (s *EventsE2ESuite) SetupSuite() {
	s.ctx = context.Background()
	t := s.T()

	// 1. Start ES container
	container, err := tcelastic.Run(
		s.ctx,
		"docker.elastic.co/elasticsearch/elasticsearch:8.10.2",
		tcelastic.WithPassword("password"),
		testcontainers.WithWaitStrategy(wait.ForLog("\"message\":\"started").WithStartupTimeout(1*time.Minute)),
	)
	require.NoError(t, err, "starting elasticsearch container")
	s.esContainer = container

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{container.Settings.Address},
		Username:  "elastic",
		Password:  "password",
		CACert:    container.Settings.CACert,
	})
	require.NoError(t, err, "creating elasticsearch client")

	// Verify connectivity
	info, err := esClient.Info()
	require.NoError(t, err, "getting elasticsearch info")
	defer info.Body.Close()

	// 2. Create index with production mapping
	res, err := esClient.Indices.Create(
		e2eEventsIndex,
		esClient.Indices.Create.WithBody(strings.NewReader(productionEventsMapping)),
	)
	require.NoError(t, err, "creating events index")
	defer res.Body.Close()
	require.False(t, res.IsError(), "creating events index should succeed: %s", res.String())

	// 3. Index seed events
	s.indexSeedEvent(esClient, seedType0Index3, "1-3")
	s.indexSeedEvent(esClient, seedType0WithCategory, "1-1")
	s.indexSeedEvent(esClient, seedType0Index2, "1-2")
	s.indexSeedEvent(esClient, seedType1, "2")
	s.indexSeedEvent(esClient, seedType2, "3")
	s.indexSeedEvent(esClient, seedType3, "4")
	s.indexSeedEvent(esClient, seedType8, "5")
	s.indexSeedEvent(esClient, seedType13, "6")

	// 4. Wire service stack: ES repo -> domain service -> gRPC server
	eventsRepo := elastic.NewEventsRepository(e2eEventsIndex, esClient)
	eventsService := domain.NewEventsService(eventsRepo)
	rpcServer := rpc.NewArchiveQueryService(nil, nil, nil, nil, eventsService, rpc.NewPageSizeLimits(1000, 10))

	// 5. Start gRPC server
	srvErrorsChan := make(chan error, 1)
	err = rpcServer.Start(rpc.StartConfig{
		ListenAddrGRPC: "127.0.0.1:0",
		MaxRecvMsgSize: 1 * 1024 * 1024,
		MaxSendMsgSize: 1 * 1024 * 1024,
	}, srvErrorsChan)
	require.NoError(t, err, "starting grpc server")
	s.srv = rpcServer

	// 6. Create gRPC client
	conn, err := grpc.NewClient(
		rpcServer.GetGRPCListenAddr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "creating grpc client")
	s.grpcClientConn = conn
	s.grpcClient = api.NewArchiveQueryServiceClient(conn)

	// 7. Create HTTP test server
	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
		MarshalOptions: protojson.MarshalOptions{EmitDefaultValues: true, EmitUnpopulated: true},
	}))
	err = api.RegisterArchiveQueryServiceHandlerServer(s.ctx, mux, rpcServer)
	require.NoError(t, err, "registering http handler")
	s.httpServer = httptest.NewServer(mux)
}

func (s *EventsE2ESuite) TearDownSuite() {
	if s.httpServer != nil {
		s.httpServer.Close()
	}
	if s.grpcClientConn != nil {
		s.grpcClientConn.Close()
	}
	if s.srv != nil {
		s.srv.Stop()
	}
	if s.esContainer != nil {
		s.esContainer.Terminate(s.ctx) //nolint: errcheck
	}
}

// --- Helpers ---

func (s *EventsE2ESuite) indexSeedEvent(esClient *elasticsearch.Client, ev seedEvent, docID string) {
	t := s.T()
	t.Helper()
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(ev)
	require.NoError(t, err, "encoding seed event")
	res, err := esClient.Index(
		e2eEventsIndex,
		&buf,
		esClient.Index.WithDocumentID(docID),
		esClient.Index.WithRefresh("true"),
	)
	require.NoError(t, err, "indexing seed event")
	defer res.Body.Close()
	require.Falsef(t, res.IsError(), "indexing seed event should succeed, got: %s", res.String())
}

func (s *EventsE2ESuite) postGetEvents(body string) (map[string]interface{}, int) {
	t := s.T()
	t.Helper()
	resp, err := http.Post(s.httpServer.URL+"/getEvents", "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &result))
	return result, resp.StatusCode
}
