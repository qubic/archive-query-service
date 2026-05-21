package entities

import api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"

type TickTransactionsResult struct {
	Hits         *Hits
	Transactions []*api.Transaction
}

func (t *TickTransactionsResult) GetHits() *Hits {
	if t == nil || t.Hits == nil {
		return &Hits{}
	}
	return t.Hits
}

func (t *TickTransactionsResult) GetTransactions() []*api.Transaction {
	if t == nil || t.Transactions == nil {
		return make([]*api.Transaction, 0)
	}
	return t.Transactions
}
