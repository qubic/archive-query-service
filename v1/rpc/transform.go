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

func TickDataToArchiveFormat(tickData TickData) (*protobuf.TickData, error) {
	sigBytes, err := base64.StdEncoding.DecodeString(tickData.Signature)
	if err != nil {
		return nil, fmt.Errorf("decoding base64 signature for tick data with number %d: %w", tickData.TickNumber, err)
	}

	varStructBytes, err := base64.StdEncoding.DecodeString(tickData.VarStruct)
	if err != nil {
		return nil, fmt.Errorf("decoding base64 varStruct for tick data with number %d: %w", tickData.TickNumber, err)
	}

	timeLockBytes, err := base64.StdEncoding.DecodeString(tickData.TimeLock)
	if err != nil {
		return nil, fmt.Errorf("decoding base64 timelock for tick data with number %d: %w", tickData.TickNumber, err)
	}

	return &protobuf.TickData{
		ComputorIndex:  tickData.ComputorIndex,
		Epoch:          tickData.Epoch,
		TickNumber:     tickData.TickNumber,
		Timestamp:      tickData.Timestamp,
		VarStruct:      varStructBytes,
		TimeLock:       timeLockBytes,
		TransactionIds: tickData.TransactionHashes,
		ContractFees:   tickData.ContractFees,
		SignatureHex:   hex.EncodeToString(sigBytes),
	}, nil
}

func ComputorsListToArchiveFormat(computorsList ComputorsList) (*protobuf.Computors, error) {

	sigBytes, err := base64.StdEncoding.DecodeString(computorsList.Signature)
	if err != nil {
		return nil, fmt.Errorf("decoding base64 computor list signature for epoch with id %d: %w", computorsList.Epoch, err)
	}

	return &protobuf.Computors{
		Epoch:        computorsList.Epoch,
		Identities:   computorsList.Identities,
		SignatureHex: hex.EncodeToString(sigBytes),
	}, nil
}
