package elastic

import (
	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
)

func transactionToAPITransaction(tx transaction) *api.Transaction {
	return &api.Transaction{
		Source:      tx.Source,
		Destination: tx.Destination,
		Amount:      tx.Amount,
		TickNumber:  tx.TickNumber,
		InputType:   tx.InputType,
		InputSize:   tx.InputSize,
		InputData:   tx.InputData,
		Signature:   tx.Signature,
		Hash:        tx.Hash,
		Timestamp:   tx.Timestamp,
		MoneyFlew:   tx.MoneyFlew,
	}
}

func transactionHitsToAPITransactions(hits []transactionHit) []*api.Transaction {
	apiTransactions := make([]*api.Transaction, len(hits))
	for i, hit := range hits {
		apiTransactions[i] = transactionToAPITransaction(hit.Source)
	}

	return apiTransactions
}

func tickDataToAPITickData(tickData tickData) *api.TickData {
	return &api.TickData{
		ComputorIndex:     tickData.ComputorIndex,
		Epoch:             tickData.Epoch,
		TickNumber:        tickData.TickNumber,
		Timestamp:         tickData.Timestamp,
		VarStruct:         tickData.VarStruct,
		TimeLock:          tickData.Timelock,
		TransactionHashes: tickData.TransactionHashes,
		ContractFees:      tickData.ContractFees,
		Signature:         tickData.Signature,
	}
}
