package test

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
	"github.com/google/go-cmp/cmp"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
)

const e2eEventsIndex = "qubic-event-logs-e2e"

const productionEventsMapping = `{
	"settings": {
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings": {
		"dynamic": "strict",
		"properties": {
			"epoch":                  { "type": "integer" },
			"tickNumber":             { "type": "unsigned_long" },
			"timestamp":              { "type": "date", "format": "epoch_millis" },
			"emittingContractIndex":  { "type": "unsigned_long" },
			"transactionHash":        { "type": "keyword", "ignore_above": 60 },
			"logId":                  { "type": "unsigned_long" },
			"logDigest":              { "type": "keyword", "ignore_above": 60 },
			"type":                   { "type": "short" },
			"category":               { "type": "byte" },
			"source":                 { "type": "keyword", "ignore_above": 60 },
			"destination":            { "type": "keyword", "ignore_above": 60 },
			"amount":                 { "type": "unsigned_long" },
			"assetName":              { "type": "keyword", "ignore_above": 60 },
			"assetIssuer":            { "type": "keyword", "ignore_above": 60 },
			"numberOfShares":         { "type": "unsigned_long" },
			"managingContractIndex":  { "type": "unsigned_long" },
			"unitOfMeasurement":      { "type": "binary" },
			"numberOfDecimalPlaces":  { "type": "byte", "index": false },
			"deductedAmount":         { "type": "unsigned_long" },
			"remainingAmount":        { "type": "long" },
			"contractIndex":          { "type": "unsigned_long" },
			"contractIndexBurnedFor": { "type": "unsigned_long" }
		}
	}
}`

// seedEvent mirrors the ES document structure (the internal event struct is unexported).
type seedEvent struct {
	Epoch                  uint32 `json:"epoch"`
	TickNumber             uint32 `json:"tickNumber"`
	Timestamp              uint64 `json:"timestamp"`
	EmittingContractIndex  uint64 `json:"emittingContractIndex"`
	TransactionHash        string `json:"transactionHash"`
	LogID                  uint64 `json:"logId"`
	LogDigest              string `json:"logDigest"`
	Type                   uint32 `json:"type"`
	Category               uint32 `json:"category"`
	Source                 string `json:"source"`
	Destination            string `json:"destination"`
	Amount                 uint64 `json:"amount"`
	AssetName              string `json:"assetName"`
	AssetIssuer            string `json:"assetIssuer"`
	NumberOfShares         uint64 `json:"numberOfShares"`
	ManagingContractIndex  uint64 `json:"managingContractIndex"`
	UnitOfMeasurement      string `json:"unitOfMeasurement"`
	NumberOfDecimalPlaces  uint32 `json:"numberOfDecimalPlaces"`
	DeductedAmount         uint64 `json:"deductedAmount"`
	RemainingAmount        int64  `json:"remainingAmount"`
	ContractIndex          uint64 `json:"contractIndex"`
	ContractIndexBurnedFor uint64 `json:"contractIndexBurnedFor"`
}

var seedType0 = seedEvent{
	Epoch: 100, TickNumber: 15000, Timestamp: 1700000001000,
	EmittingContractIndex: 1, TransactionHash: "txhash_type0_e2e",
	LogID: 1, LogDigest: "digest0", Type: 0, Category: 0,
	Source: "SRC_E2E", Destination: "DST_E2E", Amount: 5000,
}

var seedType1 = seedEvent{
	Epoch: 100, TickNumber: 15001, Timestamp: 1700000002000,
	EmittingContractIndex: 1, TransactionHash: "txhash_type1_e2e",
	LogID: 2, LogDigest: "digest1", Type: 1, Category: 1,
	AssetIssuer: "ISSUER_E2E", NumberOfShares: 1000000,
	ManagingContractIndex: 5, AssetName: "QX",
	NumberOfDecimalPlaces: 2, UnitOfMeasurement: "dW5pdHM=",
}

var seedType2 = seedEvent{
	Epoch: 100, TickNumber: 15002, Timestamp: 1700000003000,
	EmittingContractIndex: 2, TransactionHash: "txhash_type0_e2e",
	LogID: 3, LogDigest: "digest2", Type: 2, Category: 0,
	Source: "OWNER_A_E2E", Destination: "OWNER_B_E2E",
	AssetIssuer: "ISSUER_E2E", AssetName: "TOKEN", NumberOfShares: 500,
}

var seedType3 = seedEvent{
	Epoch: 101, TickNumber: 16000, Timestamp: 1700000004000,
	EmittingContractIndex: 3, TransactionHash: "txhash_type3_e2e",
	LogID: 4, LogDigest: "digest3", Type: 3, Category: 0,
	Source: "POSS_A_E2E", Destination: "POSS_B_E2E",
	AssetIssuer: "ISSUER_E2E", AssetName: "TOKEN", NumberOfShares: 300,
}

var seedType8 = seedEvent{
	Epoch: 101, TickNumber: 16001, Timestamp: 1700000005000,
	TransactionHash: "txhash_type8_e2e",
	LogID: 5, LogDigest: "digest8", Type: 8, Category: 0,
	Source: "BURNER_E2E", Amount: 9999, ContractIndexBurnedFor: 7,
}

var seedType13 = seedEvent{
	Epoch: 101, TickNumber: 16002, Timestamp: 1700000006000,
	TransactionHash: "txhash_type13_e2e",
	LogID: 6, LogDigest: "digest13", Type: 13, Category: 0,
	DeductedAmount: 50000, RemainingAmount: 100000, ContractIndex: 3,
}

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
	s.indexSeedEvent(esClient, seedType0, "1")
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

// =====================
// gRPC Tests
// =====================

func (s *EventsE2ESuite) TestGRPC_GetEvents_NoFilters() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Events, 6)
	require.Equal(t, uint32(6), resp.Hits.Total)
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_FilterByTransactionHash() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"transactionHash": "txhash_type0_e2e"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 2)
	require.Equal(t, uint32(2), resp.Hits.Total)
	for _, ev := range resp.Events {
		require.Equal(t, "txhash_type0_e2e", ev.TransactionHash)
	}
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_FilterByTickNumber() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"tickNumber": "15001"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)
	require.Equal(t, uint32(1), resp.Hits.Total)
	require.Equal(t, uint32(15001), resp.Events[0].TickNumber)
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_FilterByEventType() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"eventType": "8"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)
	require.Equal(t, uint32(1), resp.Hits.Total)
	require.Equal(t, uint32(8), resp.Events[0].EventType)
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_CombinedFilters() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"transactionHash": "txhash_type0_e2e", "eventType": "0"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)
	require.Equal(t, uint32(1), resp.Hits.Total)
	require.Equal(t, uint32(0), resp.Events[0].EventType)
	require.Equal(t, "txhash_type0_e2e", resp.Events[0].TransactionHash)
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_Pagination() {
	t := s.T()

	// Page 1
	resp1, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Pagination: &api.Pagination{Offset: 0, Size: 2},
	})
	require.NoError(t, err)
	require.Len(t, resp1.Events, 2)
	require.Equal(t, uint32(6), resp1.Hits.Total)
	require.Equal(t, uint32(0), resp1.Hits.From)
	require.Equal(t, uint32(2), resp1.Hits.Size)

	// Page 2
	resp2, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Pagination: &api.Pagination{Offset: 2, Size: 2},
	})
	require.NoError(t, err)
	require.Len(t, resp2.Events, 2)
	require.Equal(t, uint32(6), resp2.Hits.Total)
	require.Equal(t, uint32(2), resp2.Hits.From)
	require.Equal(t, uint32(2), resp2.Hits.Size)

	// Pages should have different events
	require.NotEqual(t, resp1.Events[0].LogId, resp2.Events[0].LogId)
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_EmptyResult() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"transactionHash": "nonexistent"},
	})
	require.NoError(t, err)
	require.Empty(t, resp.Events)
	require.Equal(t, uint32(0), resp.Hits.Total)
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_Type0_FullData() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"tickNumber": "15000"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)

	expected := &api.Event{
		Epoch: 100, TickNumber: 15000, Timestamp: 1700000001000,
		EmittingContractIndex: 1, TransactionHash: "txhash_type0_e2e",
		LogId: 1, LogDigest: "digest0", EventType: 0, Category: 0,
		EventData: &api.Event_QuTransfer{QuTransfer: &api.QuTransferData{
			Source: "SRC_E2E", Destination: "DST_E2E", Amount: 5000,
		}},
	}
	if diff := cmp.Diff(expected, resp.Events[0], protocmp.Transform()); diff != "" {
		require.Fail(t, "type0 event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_Type1_FullData() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"tickNumber": "15001"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)

	expected := &api.Event{
		Epoch: 100, TickNumber: 15001, Timestamp: 1700000002000,
		EmittingContractIndex: 1, TransactionHash: "txhash_type1_e2e",
		LogId: 2, LogDigest: "digest1", EventType: 1, Category: 1,
		EventData: &api.Event_AssetIssuance{AssetIssuance: &api.AssetIssuanceData{
			AssetIssuer: "ISSUER_E2E", NumberOfShares: 1000000,
			ManagingContractIndex: 5, AssetName: "QX",
			NumberOfDecimalPlaces: 2, UnitOfMeasurement: "dW5pdHM=",
		}},
	}
	if diff := cmp.Diff(expected, resp.Events[0], protocmp.Transform()); diff != "" {
		require.Fail(t, "type1 event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_Type2_FullData() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"tickNumber": "15002"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)

	expected := &api.Event{
		Epoch: 100, TickNumber: 15002, Timestamp: 1700000003000,
		EmittingContractIndex: 2, TransactionHash: "txhash_type0_e2e",
		LogId: 3, LogDigest: "digest2", EventType: 2, Category: 0,
		EventData: &api.Event_AssetOwnershipChange{AssetOwnershipChange: &api.AssetOwnershipChangeData{
			Source: "OWNER_A_E2E", Destination: "OWNER_B_E2E",
			AssetIssuer: "ISSUER_E2E", AssetName: "TOKEN", NumberOfShares: 500,
		}},
	}
	if diff := cmp.Diff(expected, resp.Events[0], protocmp.Transform()); diff != "" {
		require.Fail(t, "type2 event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_Type3_FullData() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"tickNumber": "16000"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)

	expected := &api.Event{
		Epoch: 101, TickNumber: 16000, Timestamp: 1700000004000,
		EmittingContractIndex: 3, TransactionHash: "txhash_type3_e2e",
		LogId: 4, LogDigest: "digest3", EventType: 3, Category: 0,
		EventData: &api.Event_AssetPossessionChange{AssetPossessionChange: &api.AssetPossessionChangeData{
			Source: "POSS_A_E2E", Destination: "POSS_B_E2E",
			AssetIssuer: "ISSUER_E2E", AssetName: "TOKEN", NumberOfShares: 300,
		}},
	}
	if diff := cmp.Diff(expected, resp.Events[0], protocmp.Transform()); diff != "" {
		require.Fail(t, "type3 event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_Type8_FullData() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"tickNumber": "16001"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)

	expected := &api.Event{
		Epoch: 101, TickNumber: 16001, Timestamp: 1700000005000,
		TransactionHash: "txhash_type8_e2e",
		LogId: 5, LogDigest: "digest8", EventType: 8, Category: 0,
		EventData: &api.Event_Burning{Burning: &api.BurningData{
			Source: "BURNER_E2E", Amount: 9999, ContractIndexBurnedFor: 7,
		}},
	}
	if diff := cmp.Diff(expected, resp.Events[0], protocmp.Transform()); diff != "" {
		require.Fail(t, "type8 event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_Type13_FullData() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"tickNumber": "16002"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)

	expected := &api.Event{
		Epoch: 101, TickNumber: 16002, Timestamp: 1700000006000,
		TransactionHash: "txhash_type13_e2e",
		LogId: 6, LogDigest: "digest13", EventType: 13, Category: 0,
		EventData: &api.Event_ContractReserveDeduction{ContractReserveDeduction: &api.ContractReserveDeductionData{
			DeductedAmount: 50000, RemainingAmount: 100000, ContractIndex: 3,
		}},
	}
	if diff := cmp.Diff(expected, resp.Events[0], protocmp.Transform()); diff != "" {
		require.Fail(t, "type13 event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_InvalidFilter() {
	t := s.T()
	_, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"unsupported": "value"},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_InvalidEventType() {
	t := s.T()
	_, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"eventType": "99"},
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

// =====================
// HTTP Tests
// =====================

func (s *EventsE2ESuite) TestHTTP_GetEvents_NoFilters() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 6)

	hits := result["hits"].(map[string]interface{})
	require.Equal(t, float64(6), hits["total"])
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_FilterByTransactionHash() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{"filters":{"transactionHash":"txhash_type0_e2e"}}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 2)

	hits := result["hits"].(map[string]interface{})
	require.Equal(t, float64(2), hits["total"])
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_FilterByEventType() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{"filters":{"eventType":"8"}}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_Type0_QuTransfer_FullData() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{"filters":{"tickNumber":"15000"}}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	expected := map[string]interface{}{
		"epoch": float64(100), "tickNumber": float64(15000), "timestamp": "1700000001000",
		"emittingContractIndex": "1", "transactionHash": "txhash_type0_e2e",
		"logId": "1", "logDigest": "digest0", "eventType": float64(0), "category": float64(0),
		"quTransfer": map[string]interface{}{
			"source": "SRC_E2E", "destination": "DST_E2E", "amount": "5000",
		},
	}
	if diff := cmp.Diff(expected, ev); diff != "" {
		require.Fail(t, "type0 HTTP event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_Type1_AssetIssuance_FullData() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{"filters":{"tickNumber":"15001"}}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	expected := map[string]interface{}{
		"epoch": float64(100), "tickNumber": float64(15001), "timestamp": "1700000002000",
		"emittingContractIndex": "1", "transactionHash": "txhash_type1_e2e",
		"logId": "2", "logDigest": "digest1", "eventType": float64(1), "category": float64(1),
		"assetIssuance": map[string]interface{}{
			"assetIssuer": "ISSUER_E2E", "numberOfShares": "1000000",
			"managingContractIndex": "5", "assetName": "QX",
			"numberOfDecimalPlaces": float64(2), "unitOfMeasurement": "dW5pdHM=",
		},
	}
	if diff := cmp.Diff(expected, ev); diff != "" {
		require.Fail(t, "type1 HTTP event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_Type8_Burning_FullData() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{"filters":{"tickNumber":"16001"}}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	expected := map[string]interface{}{
		"epoch": float64(101), "tickNumber": float64(16001), "timestamp": "1700000005000",
		"emittingContractIndex": "0", "transactionHash": "txhash_type8_e2e",
		"logId": "5", "logDigest": "digest8", "eventType": float64(8), "category": float64(0),
		"burning": map[string]interface{}{
			"source": "BURNER_E2E", "amount": "9999", "contractIndexBurnedFor": "7",
		},
	}
	if diff := cmp.Diff(expected, ev); diff != "" {
		require.Fail(t, "type8 HTTP event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_Type13_ContractReserveDeduction_FullData() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{"filters":{"tickNumber":"16002"}}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	expected := map[string]interface{}{
		"epoch": float64(101), "tickNumber": float64(16002), "timestamp": "1700000006000",
		"emittingContractIndex": "0", "transactionHash": "txhash_type13_e2e",
		"logId": "6", "logDigest": "digest13", "eventType": float64(13), "category": float64(0),
		"contractReserveDeduction": map[string]interface{}{
			"deductedAmount": "50000", "remainingAmount": "100000", "contractIndex": "3",
		},
	}
	if diff := cmp.Diff(expected, ev); diff != "" {
		require.Fail(t, "type13 HTTP event mismatch (-expected +actual):\n"+diff)
	}
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_EmptyResult() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{"filters":{"transactionHash":"nonexistent"}}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Empty(t, events)

	hits := result["hits"].(map[string]interface{})
	require.Equal(t, float64(0), hits["total"])
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_Pagination() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{"pagination":{"offset":0,"size":2}}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 2)

	hits := result["hits"].(map[string]interface{})
	require.Equal(t, float64(6), hits["total"])
	require.Equal(t, float64(0), hits["from"])
	require.Equal(t, float64(2), hits["size"])
}
