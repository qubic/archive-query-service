package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	rpc "github.com/qubic/archive-query-service/v2/grpc"
	"github.com/qubic/archive-query-service/v2/grpc/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestHTTPServer(t *testing.T) {
	suite.Run(t, new(HTTPServerTestSuite))
}

type HTTPServerTestSuite struct {
	suite.Suite
	server        *httptest.Server
	mockEvService *mock.MockEventsService
	mockCtrl      *gomock.Controller
}

func (s *HTTPServerTestSuite) SetupSuite() {
	ctrl := gomock.NewController(s.T())
	mockEvService := mock.NewMockEventsService(ctrl)
	rpcServer := rpc.NewArchiveQueryService(nil, nil, nil, nil, mockEvService, rpc.NewPageSizeLimits(1000, 10))

	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
		MarshalOptions: protojson.MarshalOptions{EmitDefaultValues: true, EmitUnpopulated: true},
	}))
	err := api.RegisterArchiveQueryServiceHandlerServer(context.Background(), mux, rpcServer)
	require.NoError(s.T(), err)

	s.server = httptest.NewServer(mux)
	s.mockEvService = mockEvService
	s.mockCtrl = ctrl
}

func (s *HTTPServerTestSuite) TearDownSuite() {
	if s.server != nil {
		s.server.Close()
	}
	if s.mockCtrl != nil {
		s.mockCtrl.Finish()
	}
}

var allOneofKeys = []string{"quTransfer", "assetIssuance", "assetOwnershipChange", "assetPossessionChange", "burning", "contractReserveDeduction"}

func (s *HTTPServerTestSuite) postGetEvents(body string) (map[string]interface{}, int) {
	t := s.T()
	t.Helper()
	resp, err := http.Post(s.server.URL+"/getEvents", "application/json", bytes.NewBufferString(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &result))
	return result, resp.StatusCode
}

func assertEventJSONFields(t *testing.T, eventMap map[string]interface{}, expectedOneofKey string, allKeys []string) {
	t.Helper()

	// Common fields should be present
	for _, key := range []string{"epoch", "tickNumber", "timestamp", "emittingContractIndex", "transactionHash", "logId", "logDigest", "eventType", "category"} {
		_, ok := eventMap[key]
		assert.True(t, ok, "common field %q should be present", key)
	}

	// Expected oneof field should be present
	_, ok := eventMap[expectedOneofKey]
	assert.True(t, ok, "oneof field %q should be present", expectedOneofKey)

	// Other oneof fields should be absent
	for _, key := range allKeys {
		if key == expectedOneofKey {
			continue
		}
		_, ok := eventMap[key]
		assert.False(t, ok, "oneof field %q should NOT be present for event with %q", key, expectedOneofKey)
	}
}

func (s *HTTPServerTestSuite) TestHTTP_GetEvents_Type0_QuTransfer() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&entities.EventsResult{
			Hits: &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{
				Epoch: 100, TickNumber: 15000, Timestamp: 1700000001, EmittingContractIndex: 1,
				TransactionHash: "txhash1", LogId: 1, LogDigest: "digest1", EventType: 0, Category: 0,
				EventData: &api.Event_QuTransfer{QuTransfer: &api.QuTransferData{
					Source: "SRC_IDENTITY", Destination: "DST_IDENTITY", Amount: 5000,
				}},
			}},
		}, nil)

	result, statusCode := s.postGetEvents(`{}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	assertEventJSONFields(t, ev, "quTransfer", allOneofKeys)

	qt := ev["quTransfer"].(map[string]interface{})
	assert.Equal(t, "SRC_IDENTITY", qt["source"])
	assert.Equal(t, "DST_IDENTITY", qt["destination"])
	assert.Equal(t, "5000", qt["amount"])
}

func (s *HTTPServerTestSuite) TestHTTP_GetEvents_Type1_AssetIssuance() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&entities.EventsResult{
			Hits: &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{
				Epoch: 100, TickNumber: 15001, Timestamp: 1700000002, EmittingContractIndex: 1,
				TransactionHash: "txhash2", LogId: 2, LogDigest: "digest2", EventType: 1, Category: 1,
				EventData: &api.Event_AssetIssuance{AssetIssuance: &api.AssetIssuanceData{
					AssetIssuer: "ISSUER_ID", NumberOfShares: 1000000,
					ManagingContractIndex: 5, AssetName: "QX",
					NumberOfDecimalPlaces: 2, UnitOfMeasurement: "units",
				}},
			}},
		}, nil)

	result, statusCode := s.postGetEvents(`{}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	assertEventJSONFields(t, ev, "assetIssuance", allOneofKeys)

	ai := ev["assetIssuance"].(map[string]interface{})
	assert.Equal(t, "ISSUER_ID", ai["assetIssuer"])
	assert.Equal(t, "1000000", ai["numberOfShares"])
	assert.Equal(t, "5", ai["managingContractIndex"])
	assert.Equal(t, "QX", ai["assetName"])
	assert.Equal(t, float64(2), ai["numberOfDecimalPlaces"])
	assert.Equal(t, "units", ai["unitOfMeasurement"])
}

func (s *HTTPServerTestSuite) TestHTTP_GetEvents_Type2_AssetOwnershipChange() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&entities.EventsResult{
			Hits: &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{
				Epoch: 100, TickNumber: 15002, EventType: 2,
				TransactionHash: "txhash3", LogId: 3, LogDigest: "digest3",
				EventData: &api.Event_AssetOwnershipChange{AssetOwnershipChange: &api.AssetOwnershipChangeData{
					Source: "OWNER_A", Destination: "OWNER_B", AssetIssuer: "ISSUER", AssetName: "TOKEN", NumberOfShares: 500,
				}},
			}},
		}, nil)

	result, statusCode := s.postGetEvents(`{}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	assertEventJSONFields(t, ev, "assetOwnershipChange", allOneofKeys)

	aoc := ev["assetOwnershipChange"].(map[string]interface{})
	assert.Equal(t, "OWNER_A", aoc["source"])
	assert.Equal(t, "OWNER_B", aoc["destination"])
	assert.Equal(t, "ISSUER", aoc["assetIssuer"])
	assert.Equal(t, "TOKEN", aoc["assetName"])
	assert.Equal(t, "500", aoc["numberOfShares"])
}

func (s *HTTPServerTestSuite) TestHTTP_GetEvents_Type3_AssetPossessionChange() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&entities.EventsResult{
			Hits: &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{
				Epoch: 100, TickNumber: 15003, EventType: 3,
				TransactionHash: "txhash4", LogId: 4, LogDigest: "digest4",
				EventData: &api.Event_AssetPossessionChange{AssetPossessionChange: &api.AssetPossessionChangeData{
					Source: "POSSESSOR_A", Destination: "POSSESSOR_B", AssetIssuer: "ISSUER", AssetName: "TOKEN", NumberOfShares: 300,
				}},
			}},
		}, nil)

	result, statusCode := s.postGetEvents(`{}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	assertEventJSONFields(t, ev, "assetPossessionChange", allOneofKeys)

	apc := ev["assetPossessionChange"].(map[string]interface{})
	assert.Equal(t, "POSSESSOR_A", apc["source"])
	assert.Equal(t, "POSSESSOR_B", apc["destination"])
}

func (s *HTTPServerTestSuite) TestHTTP_GetEvents_Type8_Burning() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&entities.EventsResult{
			Hits: &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{
				Epoch: 101, TickNumber: 16001, EventType: 8,
				TransactionHash: "txhash5", LogId: 5, LogDigest: "digest5",
				EventData: &api.Event_Burning{Burning: &api.BurningData{
					Source: "BURNER", Amount: 9999, ContractIndexBurnedFor: 7,
				}},
			}},
		}, nil)

	result, statusCode := s.postGetEvents(`{}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	assertEventJSONFields(t, ev, "burning", allOneofKeys)

	b := ev["burning"].(map[string]interface{})
	assert.Equal(t, "BURNER", b["source"])
	assert.Equal(t, "9999", b["amount"])
	assert.Equal(t, "7", b["contractIndexBurnedFor"])
}

func (s *HTTPServerTestSuite) TestHTTP_GetEvents_Type13_ContractReserveDeduction() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&entities.EventsResult{
			Hits: &entities.Hits{Total: 1, Relation: "eq"},
			Events: []*api.Event{{
				Epoch: 101, TickNumber: 16002, EventType: 13,
				TransactionHash: "txhash6", LogId: 6, LogDigest: "digest6",
				EventData: &api.Event_ContractReserveDeduction{ContractReserveDeduction: &api.ContractReserveDeductionData{
					DeductedAmount: 50000, RemainingAmount: 100000, ContractIndex: 3,
				}},
			}},
		}, nil)

	result, statusCode := s.postGetEvents(`{}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 1)
	ev := events[0].(map[string]interface{})

	assertEventJSONFields(t, ev, "contractReserveDeduction", allOneofKeys)

	crd := ev["contractReserveDeduction"].(map[string]interface{})
	assert.Equal(t, "50000", crd["deductedAmount"])
	assert.Equal(t, "100000", crd["remainingAmount"])
	assert.Equal(t, "3", crd["contractIndex"])
}

func (s *HTTPServerTestSuite) TestHTTP_GetEvents_MixedTypes() {
	t := s.T()
	s.mockEvService.EXPECT().GetEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&entities.EventsResult{
			Hits: &entities.Hits{Total: 3, Relation: "eq"},
			Events: []*api.Event{
				{
					Epoch: 100, TickNumber: 15000, EventType: 0,
					TransactionHash: "tx1", LogId: 1, LogDigest: "d1",
					EventData: &api.Event_QuTransfer{QuTransfer: &api.QuTransferData{
						Source: "A", Destination: "B", Amount: 100,
					}},
				},
				{
					Epoch: 100, TickNumber: 15001, EventType: 8,
					TransactionHash: "tx2", LogId: 2, LogDigest: "d2",
					EventData: &api.Event_Burning{Burning: &api.BurningData{
						Source: "C", Amount: 200, ContractIndexBurnedFor: 1,
					}},
				},
				{
					Epoch: 100, TickNumber: 15002, EventType: 13,
					TransactionHash: "tx3", LogId: 3, LogDigest: "d3",
					EventData: &api.Event_ContractReserveDeduction{ContractReserveDeduction: &api.ContractReserveDeductionData{
						DeductedAmount: 300, RemainingAmount: 700, ContractIndex: 2,
					}},
				},
			},
		}, nil)

	result, statusCode := s.postGetEvents(`{}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 3)

	// First event: quTransfer
	ev0 := events[0].(map[string]interface{})
	assertEventJSONFields(t, ev0, "quTransfer", allOneofKeys)

	// Second event: burning
	ev1 := events[1].(map[string]interface{})
	assertEventJSONFields(t, ev1, "burning", allOneofKeys)

	// Third event: contractReserveDeduction
	ev2 := events[2].(map[string]interface{})
	assertEventJSONFields(t, ev2, "contractReserveDeduction", allOneofKeys)
}
