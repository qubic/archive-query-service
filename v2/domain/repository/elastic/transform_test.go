package elastic

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

func Test_eventToAPIEvent_BasicFields(t *testing.T) {
	txHash := "abc123"
	e := event{
		Epoch:           1,
		TickNumber:      100,
		Timestamp:       1000,
		TransactionHash: &txHash,
		LogID:           123,
		LogDigest:       "digest",
		LogType:         0,
		Categories:      []int32{},
	}

	apiEv := eventToAPIEvent(e)

	expected := &api.Event{
		Epoch:           1,
		TickNumber:      100,
		Timestamp:       1000,
		TransactionHash: &txHash,
		LogId:           123,
		LogDigest:       "digest",
		LogType:         0,
		Categories:      []int32{},
		EventData:       &api.Event_QuTransfer{QuTransfer: &api.QuTransferData{}},
	}
	diff := cmp.Diff(expected, apiEv, protocmp.Transform())
	require.Empty(t, diff, "mismatch (-expected +actual):\n"+diff)
}

func Test_eventToAPIEvent_SmartContractMessage(t *testing.T) {
	txHash := "abc123"
	tests := []struct {
		name    string
		logType uint32
	}{
		{"Type 4", 4},
		{"Type 5", 5},
		{"Type 6", 6},
		{"Type 7", 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := event{
				TransactionHash:     &txHash,
				LogType:             tt.logType,
				ContractIndex:       10,
				ContractMessageType: 20,
				RawPayload:          []byte{0x01, 0x02, 0x03},
			}

			apiEv := eventToAPIEvent(e)

			expected := &api.Event{
				TransactionHash: &txHash,
				LogType:         tt.logType,
				RawPayload:      []byte{0x01, 0x02, 0x03},
				EventData: &api.Event_SmartContractMessage{SmartContractMessage: &api.SmartContractMessageData{
					ContractIndex:       10,
					ContractMessageType: 20,
				}},
			}
			diff := cmp.Diff(expected, apiEv, protocmp.Transform())
			require.Empty(t, diff, "mismatch (-expected +actual):\n"+diff)
		})
	}
}

func Test_eventToAPIEvent_CustomMessage(t *testing.T) {
	e := event{
		LogType:       255,
		CustomMessage: 6217575821008262227,
	}

	apiEv := eventToAPIEvent(e)

	expected := &api.Event{
		LogType: 255,
		EventData: &api.Event_CustomMessage{CustomMessage: &api.CustomMessageData{
			Value: 6217575821008262227,
		}},
	}
	diff := cmp.Diff(expected, apiEv, protocmp.Transform())
	require.Empty(t, diff, "mismatch (-expected +actual):\n"+diff)
}

func Test_eventToAPIEvent_RawTypes(t *testing.T) {
	tests := []struct {
		name    string
		logType uint32
	}{
		{"Type 9 - dust burning", 9},
		{"Type 10 - spectrum_stats", 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := event{
				LogType:    tt.logType,
				RawPayload: []byte{0x01, 0x02, 0x03, 0x04},
			}

			apiEv := eventToAPIEvent(e)

			expected := &api.Event{
				LogType:    tt.logType,
				RawPayload: []byte{0x01, 0x02, 0x03, 0x04},
			}
			diff := cmp.Diff(expected, apiEv, protocmp.Transform())
			require.Empty(t, diff, "mismatch (-expected +actual):\n"+diff)
		})
	}
}

func Test_eventToAPIEvent_Type11(t *testing.T) {
	e := event{
		LogType: 11, AssetName: "TOKEN", AssetIssuer: "ISSUER",
		Owner: "OWNER", NumberOfShares: 750,
		SourceContractIndex: 1, DestinationContractIndex: 2,
	}

	apiEv := eventToAPIEvent(e)

	expected := &api.Event{
		LogType: 11,
		EventData: &api.Event_AssetOwnershipManagingContractChange{
			AssetOwnershipManagingContractChange: &api.AssetOwnershipManagingContractChangeData{
				AssetName:                "TOKEN",
				AssetIssuer:              "ISSUER",
				Owner:                    "OWNER",
				NumberOfShares:           750,
				SourceContractIndex:      1,
				DestinationContractIndex: 2,
			},
		},
	}
	diff := cmp.Diff(expected, apiEv, protocmp.Transform())
	require.Empty(t, diff, "mismatch (-expected +actual):\n"+diff)
}

func Test_eventToAPIEvent_Type12(t *testing.T) {
	e := event{
		LogType: 12, AssetName: "TOKEN", AssetIssuer: "ISSUER",
		Owner: "OWNER", Possessor: "POSSESSOR", NumberOfShares: 400,
		SourceContractIndex: 3, DestinationContractIndex: 4,
	}

	apiEv := eventToAPIEvent(e)

	expected := &api.Event{
		LogType: 12,
		EventData: &api.Event_AssetPossessionManagingContractChange{
			AssetPossessionManagingContractChange: &api.AssetPossessionManagingContractChangeData{
				AssetName:                "TOKEN",
				AssetIssuer:              "ISSUER",
				Owner:                    "OWNER",
				Possessor:                "POSSESSOR",
				NumberOfShares:           400,
				SourceContractIndex:      3,
				DestinationContractIndex: 4,
			},
		},
	}
	diff := cmp.Diff(expected, apiEv, protocmp.Transform())
	require.Empty(t, diff, "mismatch (-expected +actual):\n"+diff)
}
