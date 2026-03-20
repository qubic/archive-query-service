package elastic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	assert.Equal(t, e.Epoch, apiEv.Epoch)
	assert.Equal(t, e.TickNumber, apiEv.TickNumber)
	assert.Equal(t, e.Timestamp, apiEv.Timestamp)
	assert.Equal(t, *e.TransactionHash, apiEv.GetTransactionHash())
	assert.Equal(t, e.LogID, apiEv.LogId)
	assert.Equal(t, e.LogDigest, apiEv.LogDigest)
	assert.Equal(t, e.LogType, apiEv.LogType)
	assert.Equal(t, e.Categories, apiEv.Categories)
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

			assert.Equal(t, *e.TransactionHash, apiEv.GetTransactionHash())
			assert.Equal(t, e.LogType, apiEv.LogType)

			scMsg := apiEv.GetSmartContractMessage()
			assert.NotNil(t, scMsg)
			assert.Equal(t, e.ContractIndex, scMsg.ContractIndex)
			assert.Equal(t, e.ContractMessageType, scMsg.ContractMessageType)
			assert.Equal(t, e.RawPayload, apiEv.RawPayload) // set for smart contract messages
		})
	}
}

func Test_eventToAPIEvent_CustomMessage(t *testing.T) {
	e := event{
		LogType:       255,
		CustomMessage: 6217575821008262227,
	}

	apiEv := eventToAPIEvent(e)

	assert.Equal(t, e.LogType, apiEv.LogType)

	customMsg := apiEv.GetCustomMessage()
	require.NotNil(t, customMsg)
	assert.Equal(t, e.CustomMessage, customMsg.Value)
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

			assert.Equal(t, e.LogType, apiEv.LogType)
			assert.Equal(t, e.RawPayload, apiEv.RawPayload)
			assert.Nil(t, apiEv.EventData) // no specific event data for these types
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
	data := apiEv.GetAssetOwnershipManagingContractChange()
	require.NotNil(t, data)
	assert.Equal(t, "TOKEN", data.AssetName)
	assert.Equal(t, "ISSUER", data.AssetIssuer)
	assert.Equal(t, "OWNER", data.Owner)
	assert.Equal(t, uint64(750), data.NumberOfShares)
	assert.Equal(t, uint64(1), data.SourceContractIndex)
	assert.Equal(t, uint64(2), data.DestinationContractIndex)
}

func Test_eventToAPIEvent_Type12(t *testing.T) {
	e := event{
		LogType: 12, AssetName: "TOKEN", AssetIssuer: "ISSUER",
		Owner: "OWNER", Possessor: "POSSESSOR", NumberOfShares: 400,
		SourceContractIndex: 3, DestinationContractIndex: 4,
	}
	apiEv := eventToAPIEvent(e)
	data := apiEv.GetAssetPossessionManagingContractChange()
	require.NotNil(t, data)
	assert.Equal(t, "TOKEN", data.AssetName)
	assert.Equal(t, "ISSUER", data.AssetIssuer)
	assert.Equal(t, "OWNER", data.Owner)
	assert.Equal(t, "POSSESSOR", data.Possessor)
	assert.Equal(t, uint64(400), data.NumberOfShares)
	assert.Equal(t, uint64(3), data.SourceContractIndex)
	assert.Equal(t, uint64(4), data.DestinationContractIndex)
}
