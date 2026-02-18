package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	rpc "github.com/qubic/archive-query-service/v2/grpc"
	"github.com/qubic/archive-query-service/v2/grpc/mock"
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

func requireEventJSONFields(
	t *testing.T,
	eventMap map[string]interface{},
	expectedCommon map[string]interface{},
	expectedOneofKey string,
	expectedOneofData map[string]interface{},
	allKeys []string,
) {
	t.Helper()

	// Build actual common-field map from eventMap
	commonKeys := []string{"epoch", "tickNumber", "timestamp", "emittingContractIndex", "transactionHash", "logId", "logDigest", "eventType", "category"}
	actualCommon := make(map[string]interface{}, len(commonKeys))
	for _, key := range commonKeys {
		val, ok := eventMap[key]
		require.True(t, ok, "common field %q should be present", key)
		actualCommon[key] = val
	}
	if diff := cmp.Diff(expectedCommon, actualCommon); diff != "" {
		require.Fail(t, "common fields mismatch (-expected +actual):\n"+diff)
	}

	// Compare oneof sub-message
	actualOneof, ok := eventMap[expectedOneofKey]
	require.True(t, ok, "oneof field %q should be present", expectedOneofKey)
	actualOneofMap, ok := actualOneof.(map[string]interface{})
	require.True(t, ok, "oneof field %q should be a map", expectedOneofKey)
	if diff := cmp.Diff(expectedOneofData, actualOneofMap); diff != "" {
		require.Fail(t, "oneof field "+expectedOneofKey+" mismatch (-expected +actual):\n"+diff)
	}

	// Other oneof fields should be absent
	for _, key := range allKeys {
		if key == expectedOneofKey {
			continue
		}
		_, present := eventMap[key]
		require.False(t, present, "oneof field %q should NOT be present for event with %q", key, expectedOneofKey)
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

	requireEventJSONFields(t, ev,
		map[string]interface{}{
			"epoch": float64(100), "tickNumber": float64(15000), "timestamp": "1700000001",
			"emittingContractIndex": "1", "transactionHash": "txhash1",
			"logId": "1", "logDigest": "digest1", "eventType": float64(0), "category": float64(0),
		},
		"quTransfer",
		map[string]interface{}{
			"source": "SRC_IDENTITY", "destination": "DST_IDENTITY", "amount": "5000",
		},
		allOneofKeys,
	)
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

	requireEventJSONFields(t, ev,
		map[string]interface{}{
			"epoch": float64(100), "tickNumber": float64(15001), "timestamp": "1700000002",
			"emittingContractIndex": "1", "transactionHash": "txhash2",
			"logId": "2", "logDigest": "digest2", "eventType": float64(1), "category": float64(1),
		},
		"assetIssuance",
		map[string]interface{}{
			"assetIssuer": "ISSUER_ID", "numberOfShares": "1000000",
			"managingContractIndex": "5", "assetName": "QX",
			"numberOfDecimalPlaces": float64(2), "unitOfMeasurement": "units",
		},
		allOneofKeys,
	)
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

	requireEventJSONFields(t, ev,
		map[string]interface{}{
			"epoch": float64(100), "tickNumber": float64(15002), "timestamp": "0",
			"emittingContractIndex": "0", "transactionHash": "txhash3",
			"logId": "3", "logDigest": "digest3", "eventType": float64(2), "category": float64(0),
		},
		"assetOwnershipChange",
		map[string]interface{}{
			"source": "OWNER_A", "destination": "OWNER_B",
			"assetIssuer": "ISSUER", "assetName": "TOKEN", "numberOfShares": "500",
		},
		allOneofKeys,
	)
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

	requireEventJSONFields(t, ev,
		map[string]interface{}{
			"epoch": float64(100), "tickNumber": float64(15003), "timestamp": "0",
			"emittingContractIndex": "0", "transactionHash": "txhash4",
			"logId": "4", "logDigest": "digest4", "eventType": float64(3), "category": float64(0),
		},
		"assetPossessionChange",
		map[string]interface{}{
			"source": "POSSESSOR_A", "destination": "POSSESSOR_B",
			"assetIssuer": "ISSUER", "assetName": "TOKEN", "numberOfShares": "300",
		},
		allOneofKeys,
	)
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

	requireEventJSONFields(t, ev,
		map[string]interface{}{
			"epoch": float64(101), "tickNumber": float64(16001), "timestamp": "0",
			"emittingContractIndex": "0", "transactionHash": "txhash5",
			"logId": "5", "logDigest": "digest5", "eventType": float64(8), "category": float64(0),
		},
		"burning",
		map[string]interface{}{
			"source": "BURNER", "amount": "9999", "contractIndexBurnedFor": "7",
		},
		allOneofKeys,
	)
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

	requireEventJSONFields(t, ev,
		map[string]interface{}{
			"epoch": float64(101), "tickNumber": float64(16002), "timestamp": "0",
			"emittingContractIndex": "0", "transactionHash": "txhash6",
			"logId": "6", "logDigest": "digest6", "eventType": float64(13), "category": float64(0),
		},
		"contractReserveDeduction",
		map[string]interface{}{
			"deductedAmount": "50000", "remainingAmount": "100000", "contractIndex": "3",
		},
		allOneofKeys,
	)
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
	requireEventJSONFields(t, ev0,
		map[string]interface{}{
			"epoch": float64(100), "tickNumber": float64(15000), "timestamp": "0",
			"emittingContractIndex": "0", "transactionHash": "tx1",
			"logId": "1", "logDigest": "d1", "eventType": float64(0), "category": float64(0),
		},
		"quTransfer",
		map[string]interface{}{
			"source": "A", "destination": "B", "amount": "100",
		},
		allOneofKeys,
	)

	// Second event: burning
	ev1 := events[1].(map[string]interface{})
	requireEventJSONFields(t, ev1,
		map[string]interface{}{
			"epoch": float64(100), "tickNumber": float64(15001), "timestamp": "0",
			"emittingContractIndex": "0", "transactionHash": "tx2",
			"logId": "2", "logDigest": "d2", "eventType": float64(8), "category": float64(0),
		},
		"burning",
		map[string]interface{}{
			"source": "C", "amount": "200", "contractIndexBurnedFor": "1",
		},
		allOneofKeys,
	)

	// Third event: contractReserveDeduction
	ev2 := events[2].(map[string]interface{})
	requireEventJSONFields(t, ev2,
		map[string]interface{}{
			"epoch": float64(100), "tickNumber": float64(15002), "timestamp": "0",
			"emittingContractIndex": "0", "transactionHash": "tx3",
			"logId": "3", "logDigest": "d3", "eventType": float64(13), "category": float64(0),
		},
		"contractReserveDeduction",
		map[string]interface{}{
			"deductedAmount": "300", "remainingAmount": "700", "contractIndex": "2",
		},
		allOneofKeys,
	)
}
