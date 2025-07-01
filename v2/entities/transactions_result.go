package entities

import api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"

type TransactionsResult struct {
	LastProcessedTick uint32
	Hits              *Hits
	Transactions      []*api.Transaction
}

func (t *TransactionsResult) GetHits() *Hits {
	if t == nil || t.Hits == nil {
		return &Hits{}
	}
	return t.Hits
}

func (t *TransactionsResult) GetTransactions() []*api.Transaction {
	if t == nil || t.Transactions == nil {
		return make([]*api.Transaction, 0)
	}
	return t.Transactions
}
