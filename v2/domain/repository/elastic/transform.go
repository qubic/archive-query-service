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

func computorsListHitsToAPIObjects(hits []computorsListHit) []*api.ComputorList {
	computorsLists := make([]*api.ComputorList, len(hits))
	for i, hit := range hits {
		computorsLists[i] = computorsListToAPIObject(hit.Source)
	}

	return computorsLists
}

func computorsListToAPIObject(cl computorsList) *api.ComputorList {
	return &api.ComputorList{
		Epoch:      cl.Epoch,
		TickNumber: cl.TickNumber,
		Identities: cl.Identities,
		Signature:  cl.Signature,
	}
}

func eventToAPIEvent(e event) *api.Event {
	ev := &api.Event{
		Epoch:           e.Epoch,
		TickNumber:      e.TickNumber,
		Timestamp:       e.Timestamp,
		TransactionHash: e.TransactionHash,
		LogId:           e.LogID,
		LogDigest:       e.LogDigest,
		EventType:       e.Type,
		Categories:      e.Categories,
	}
	switch e.Type {
	case 0:
		ev.EventData = &api.Event_QuTransfer{QuTransfer: &api.QuTransferData{
			Source: e.Source, Destination: e.Destination, Amount: e.Amount,
		}}
	case 1:
		ev.EventData = &api.Event_AssetIssuance{AssetIssuance: &api.AssetIssuanceData{
			AssetIssuer: e.AssetIssuer, NumberOfShares: e.NumberOfShares,
			ManagingContractIndex: e.ManagingContractIndex, AssetName: e.AssetName,
			NumberOfDecimalPlaces: e.NumberOfDecimalPlaces, UnitOfMeasurement: e.UnitOfMeasurement,
		}}
	case 2:
		ev.EventData = &api.Event_AssetOwnershipChange{AssetOwnershipChange: &api.AssetOwnershipChangeData{
			Source: e.Source, Destination: e.Destination, AssetIssuer: e.AssetIssuer,
			AssetName: e.AssetName, NumberOfShares: e.NumberOfShares,
		}}
	case 3:
		ev.EventData = &api.Event_AssetPossessionChange{AssetPossessionChange: &api.AssetPossessionChangeData{
			Source: e.Source, Destination: e.Destination, AssetIssuer: e.AssetIssuer,
			AssetName: e.AssetName, NumberOfShares: e.NumberOfShares,
		}}
	case 8:
		ev.EventData = &api.Event_Burning{Burning: &api.BurningData{
			Source: e.Source, Amount: e.Amount, ContractIndexBurnedFor: e.ContractIndexBurnedFor,
		}}
	case 13:
		ev.EventData = &api.Event_ContractReserveDeduction{ContractReserveDeduction: &api.ContractReserveDeductionData{
			DeductedAmount: e.DeductedAmount, RemainingAmount: e.RemainingAmount, ContractIndex: e.ContractIndex,
		}}
	}
	return ev
}

func eventHitsToAPIEvents(hits []eventHit) []*api.Event {
	events := make([]*api.Event, len(hits))
	for i, hit := range hits {
		events[i] = eventToAPIEvent(hit.Source)
	}
	return events
}
