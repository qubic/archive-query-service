package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/domain"
)

type transactionGetResponse struct {
	Index       string      `json:"_index"`
	Id          string      `json:"_id"`
	Version     int         `json:"_version"`
	SeqNo       int         `json:"_seq_no"`
	PrimaryTerm int         `json:"_primary_term"`
	Found       bool        `json:"found"`
	Source      transaction `json:"_source"`
}

type transactionHit struct {
	Source transaction `json:"_source"`
}

type transactionsSearchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []transactionHit `json:"hits"`
	} `json:"hits"`
}

type transaction struct {
	Hash        string `json:"hash"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      int64  `json:"amount"`
	TickNumber  uint32 `json:"tickNumber"`
	InputType   uint32 `json:"inputType"`
	InputSize   uint32 `json:"inputSize"`
	InputData   string `json:"inputData"`
	Signature   string `json:"signature"`
	Timestamp   uint64 `json:"timestamp"`
	MoneyFlew   bool   `json:"moneyFlew"`
}

func (r *Repository) GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error) {
	res, err := r.esClient.Get(r.txIndex, hash)
	if err != nil {
		return nil, fmt.Errorf("calling es client get with: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, domain.ErrNotFound
	}

	if res.IsError() {
		return nil, fmt.Errorf("got error response from Elasticsearch: %s", res.String())
	}

	var result transactionGetResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding json response: %w", err)
	}

	return transactionToApiTransaction(result.Source), nil
}

func (r *Repository) GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32) ([]*api.Transaction, error) {
	query, err := createTickTransactionsQuery(tickNumber)
	if err != nil {
		return nil, fmt.Errorf("creating query: %w", err)
	}

	res, err := r.esClient.Search(
		r.esClient.Search.WithContext(ctx),
		r.esClient.Search.WithIndex(r.txIndex),
		r.esClient.Search.WithBody(&query),
		r.esClient.Search.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("got error response from Elasticsearch: %s", res.String())
	}

	var result transactionsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return transactionHitsToApiTransactions(result.Hits.Hits), nil
}

// GetTransactionsForIdentity - method interface is subject to change after changing pagination logic
func (r *Repository) GetTransactionsForIdentity(ctx context.Context, identity string, maxTick uint32, pageSize, pageNumber int, desc bool) ([]*api.Transaction, error) {
	query, err := createIdentitiesQuery(identity, pageSize, pageNumber, desc, maxTick)
	if err != nil {
		return nil, fmt.Errorf("creating query: %w", err)
	}

	res, err := r.esClient.Search(
		r.esClient.Search.WithContext(ctx),
		r.esClient.Search.WithIndex(r.txIndex),
		r.esClient.Search.WithBody(&query),
		r.esClient.Search.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	var result transactionsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return transactionHitsToApiTransactions(result.Hits.Hits), nil
}

func createTickTransactionsQuery(tick uint32) (bytes.Buffer, error) {
	query := map[string]interface{}{
		"track_total_hits": "true",
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"tickNumber": tick,
			},
		},
		"size": 1024,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return bytes.Buffer{}, fmt.Errorf("encoding query: %w", err)
	}

	return buf, nil
}

func createIdentitiesQuery(identity string, pageSize, pageNumber int, desc bool, maxTick uint32) (bytes.Buffer, error) {
	from := pageNumber * pageSize
	querySort := "asc"
	if desc {
		querySort = "desc"
	}

	tickNumberRangeFilter := map[string]interface{}{
		"lte": maxTick,
		//"gte": 10000000, // min tick can also be implemented if needed
	}
	if maxTick <= 0 {
		delete(tickNumberRangeFilter, "lte")
	}

	query := map[string]interface{}{
		"track_total_hits": "10000", // limit to max page size for better performance
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []interface{}{
					map[string]interface{}{
						"match": map[string]interface{}{
							"source": identity,
						},
					},
					map[string]interface{}{
						"match": map[string]interface{}{
							"destination": identity,
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"tickNumber": tickNumberRangeFilter,
						},
					},
				},
				"minimum_should_match": 1,
			},
		},
		"size": pageSize,
		"from": from,
		"sort": []interface{}{
			map[string]interface{}{
				"timestamp": querySort,
			},
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return bytes.Buffer{}, fmt.Errorf("encoding query: %w", err)
	}

	return buf, nil
}
