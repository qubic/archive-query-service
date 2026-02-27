package integration

import (
	"net/http"

	"github.com/google/go-cmp/cmp"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

// =====================
// gRPC Tests
// =====================

func (s *EventsE2ESuite) TestGRPC_GetEvents_NoFilters() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Events, 8)
	require.Equal(t, uint32(8), resp.Hits.Total)
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_FilterByTransactionHash() {
	t := s.T()
	resp, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Filters: map[string]string{"transactionHash": "zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 2)
	require.Equal(t, uint32(2), resp.Hits.Total)
	for _, ev := range resp.Events {
		require.Equal(t, "zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh", ev.TransactionHash)
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
		Filters: map[string]string{"transactionHash": "zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh", "eventType": "0"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Events, 1)
	require.Equal(t, uint32(1), resp.Hits.Total)
	require.Equal(t, uint32(0), resp.Events[0].EventType)
	require.Equal(t, "zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh", resp.Events[0].TransactionHash)
}

func (s *EventsE2ESuite) TestGRPC_GetEvents_Pagination() {
	t := s.T()

	// Page 1
	resp1, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Pagination: &api.Pagination{Offset: 0, Size: 2},
	})
	require.NoError(t, err)
	require.Len(t, resp1.Events, 2)
	require.Equal(t, uint32(8), resp1.Hits.Total)
	require.Equal(t, uint32(0), resp1.Hits.From)
	require.Equal(t, uint32(2), resp1.Hits.Size)

	// Page 2
	resp2, err := s.grpcClient.GetEvents(t.Context(), &api.GetEventsRequest{
		Pagination: &api.Pagination{Offset: 2, Size: 2},
	})
	require.NoError(t, err)
	require.Len(t, resp2.Events, 2)
	require.Equal(t, uint32(8), resp2.Hits.Total)
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
	require.Len(t, resp.Events, 3)

	expected := &api.Event{
		Epoch: 100, TickNumber: 15000, Timestamp: 1700000001000,
		TransactionHash: "zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh",
		LogId:           1, LogDigest: "digest0", EventType: 0,
		EventData: &api.Event_QuTransfer{QuTransfer: &api.QuTransferData{
			Source: "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", Destination: "BZBQFLLBNCXEMGQOUAPQYSWCBHRBJJFHFFLSENFLEVKEIYVHDSOFWKUUPGJD", Amount: 5000,
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
		TransactionHash: "atrpnwqfgkjlbchsdyeimxouvzatrpnwqfgkjlbchsdyeimxouvzatrpnwqf",
		LogId:           2, LogDigest: "digest1", EventType: 1,
		EventData: &api.Event_AssetIssuance{AssetIssuance: &api.AssetIssuanceData{
			AssetIssuer: "CFBMEMZOIDEXQAUXYYSZIURADQLAPWPMNJPBCGFDLXDIBITCOULXPAJFNAJK", NumberOfShares: 1000000,
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
		TransactionHash: "zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh",
		LogId:           3, LogDigest: "digest2", EventType: 2,
		EventData: &api.Event_AssetOwnershipChange{AssetOwnershipChange: &api.AssetOwnershipChangeData{
			Source: "DLRMHGPFARAKPFLBCIFGQWFPMFPAQKESVFAIGGHFXQFBKGMUBBGPCJFKNMMD", Destination: "EPFNIJQGQBSLQLGDDJGHRGQNGOBRLFRTGHBHIJGYLRGCLHJOCCQDHGKLONNE",
			AssetIssuer: "CFBMEMZOIDEXQAUXYYSZIURADQLAPWPMNJPBCGFDLXDIBITCOULXPAJFNAJK", AssetName: "TOKEN", NumberOfShares: 500,
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
		TransactionHash: "bkuedoxghrlmcfitjwangpyqzbkuedoxghrlmcfitjwangpyqzbkuedoxghr",
		LogId:           4, LogDigest: "digest3", EventType: 3,
		EventData: &api.Event_AssetPossessionChange{AssetPossessionChange: &api.AssetPossessionChangeData{
			Source: "FQGOKLRHRCTNRMHEEKHIBRHOPHCSMGSUHIBIJKHZMSHDMNKIPDREIHHLPPPF", Destination: "GRHPLMSISDUPSNIFFLKJCSIPQIDTNHTVIJCJKLIANSKENLLJQESFJIIMQQRG",
			AssetIssuer: "CFBMEMZOIDEXQAUXYYSZIURADQLAPWPMNJPBCGFDLXDIBITCOULXPAJFNAJK", AssetName: "TOKEN", NumberOfShares: 300,
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
		TransactionHash: "cmvfepyihksndgjtuxbohrqzacmvfepyihksndgjtuxbohrqzacmvfepyihks",
		LogId:           5, LogDigest: "digest8", EventType: 8,
		EventData: &api.Event_Burning{Burning: &api.BurningData{
			Source: "HSIQQNTTJTEVRPOJGGMLKDSQRJEUPIUWJKDKLMJBTOLFOMMMKRFTGKKJNRSH", Amount: 9999, ContractIndexBurnedFor: 7,
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
		TransactionHash: "dnwgfqzjiltoehukvycpiskabdnwgfqzjiltoehukvycpiskabdnwgfqzjilt",
		LogId:           6, LogDigest: "digest13", EventType: 13,
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
	require.Len(t, events, 8)

	hits := result["hits"].(map[string]interface{})
	require.Equal(t, float64(8), hits["total"])
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_VerifySortOrder() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{}`)
	require.Equal(t, http.StatusOK, statusCode)

	events := result["events"].([]interface{})
	require.Len(t, events, 8)

	// last three are in same tick (tick number descending)
	require.Equal(t, events[5].(map[string]interface{})["tickNumber"], events[6].(map[string]interface{})["tickNumber"])
	require.Equal(t, events[5].(map[string]interface{})["tickNumber"], events[7].(map[string]interface{})["tickNumber"])
	// they are in the correct log id order (ascending)
	assert.Equal(t, events[5].(map[string]interface{})["logId"], "1")
	assert.Equal(t, events[6].(map[string]interface{})["logId"], "2")
	assert.Equal(t, events[7].(map[string]interface{})["logId"], "3")
}

func (s *EventsE2ESuite) TestHTTP_GetEvents_FilterByTransactionHash() {
	t := s.T()
	result, statusCode := s.postGetEvents(`{"filters":{"transactionHash":"zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh"}}`)
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
	require.Len(t, events, 3)
	ev := events[0].(map[string]interface{})

	expected := map[string]interface{}{
		"epoch": float64(100), "tickNumber": float64(15000), "timestamp": "1700000001000",
		"transactionHash": "zycobqjpgdcagflcvgtkboafbryahgjbbwhgjjlblhzocwncjhhjshqfsndh",
		"logId":           "1", "logDigest": "digest0", "eventType": float64(0),
		"quTransfer": map[string]interface{}{
			"source": "QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", "destination": "BZBQFLLBNCXEMGQOUAPQYSWCBHRBJJFHFFLSENFLEVKEIYVHDSOFWKUUPGJD", "amount": "5000",
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
		"transactionHash": "atrpnwqfgkjlbchsdyeimxouvzatrpnwqfgkjlbchsdyeimxouvzatrpnwqf",
		"logId":           "2", "logDigest": "digest1", "eventType": float64(1),
		"assetIssuance": map[string]interface{}{
			"assetIssuer": "CFBMEMZOIDEXQAUXYYSZIURADQLAPWPMNJPBCGFDLXDIBITCOULXPAJFNAJK", "numberOfShares": "1000000",
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
		"transactionHash": "cmvfepyihksndgjtuxbohrqzacmvfepyihksndgjtuxbohrqzacmvfepyihks",
		"logId":           "5", "logDigest": "digest8", "eventType": float64(8),
		"burning": map[string]interface{}{
			"source": "HSIQQNTTJTEVRPOJGGMLKDSQRJEUPIUWJKDKLMJBTOLFOMMMKRFTGKKJNRSH", "amount": "9999", "contractIndexBurnedFor": "7",
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
		"transactionHash": "dnwgfqzjiltoehukvycpiskabdnwgfqzjiltoehukvycpiskabdnwgfqzjilt",
		"logId":           "6", "logDigest": "digest13", "eventType": float64(13),
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
	require.Equal(t, float64(8), hits["total"])
	require.Equal(t, float64(0), hits["from"])
	require.Equal(t, float64(2), hits["size"])
}
