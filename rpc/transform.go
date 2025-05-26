package rpc

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/qubic/archive-query-service/protobuf"
)

func TxToArchivePartialFormat(tx Tx) (*protobuf.Transaction, error) {
	inputBytes, err := base64.StdEncoding.DecodeString(tx.InputData)
	if err != nil {
		return nil, fmt.Errorf("decoding base64 input for tx with id %s: %w", tx.Hash, err)
	}

	sigBytes, err := base64.StdEncoding.DecodeString(tx.Signature)
	if err != nil {
		return nil, fmt.Errorf("decoding base64 signature for tx with id %s: %w", tx.Hash, err)
	}

	return &protobuf.Transaction{
		SourceId:     tx.Source,
		DestId:       tx.Destination,
		Amount:       tx.Amount,
		TickNumber:   tx.TickNumber,
		InputType:    tx.InputType,
		InputSize:    tx.InputSize,
		InputHex:     hex.EncodeToString(inputBytes),
		SignatureHex: hex.EncodeToString(sigBytes),
		TxId:         tx.Hash,
	}, nil
}

func TxToArchiveFullFormat(tx Tx) (*protobuf.TransactionData, error) {
	partialTx, err := TxToArchivePartialFormat(tx)
	if err != nil {
		return nil, fmt.Errorf("converting tx to partial format: %w", err)
	}

	return &protobuf.TransactionData{
		Transaction: partialTx,
		Timestamp:   tx.Timestamp,
		MoneyFlew:   tx.MoneyFlew,
	}, nil
}

func TxToNewFormat(tx Tx) *protobuf.NewTransaction {
	return &protobuf.NewTransaction{
		SourceId:   tx.Source,
		DestId:     tx.Destination,
		Amount:     tx.Amount,
		TickNumber: tx.TickNumber,
		InputType:  tx.InputType,
		InputSize:  tx.InputSize,
		Input:      tx.InputData,
		Signature:  tx.Signature,
		TxId:       tx.Hash,
		Timestamp:  tx.Timestamp,
		MoneyFlew:  tx.MoneyFlew,
	}
}
