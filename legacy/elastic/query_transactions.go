package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

func (c *Client) QueryIdentityTransactions(ctx context.Context, ID string, pageSize, pageNumber int, desc bool, startTick, endTick uint32) (result TransactionsSearchResponse, err error) {

	query, err := createIdentitiesQuery(ID, pageSize, pageNumber, desc, startTick, endTick)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("creating query: %w", err)
	}

	res, err := c.elastic.Search(
		c.elastic.Search.WithContext(ctx),
		c.elastic.Search.WithIndex(c.txIndex),
		c.elastic.Search.WithBody(&query),
	)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return TransactionsSearchResponse{}, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("decoding response: %w", err)
	}

	return result, nil
}

func (c *Client) QueryTransactionByHash(ctx context.Context, txID string) (result TransactionGetResponse, err error) {
	res, err := c.elastic.Get(
		c.txIndex,
		txID,
		c.elastic.Get.WithContext(ctx),
	)
	if err != nil {
		return TransactionGetResponse{}, fmt.Errorf("calling es client get: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return TransactionGetResponse{}, ErrDocumentNotFound
	}

	if res.IsError() {
		return TransactionGetResponse{}, fmt.Errorf("got error response from Elasticsearch: %s", res.String())
	}

	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return TransactionGetResponse{}, fmt.Errorf("decoding response: %w", err)
	}

	return result, nil
}

func (c *Client) QueryTickTransactions(ctx context.Context, tick uint32) (TransactionsSearchResponse, error) {
	query, err := createTickTransactionsQuery(tick)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("creating query: %w", err)
	}

	res, err := c.elastic.Search(
		c.elastic.Search.WithContext(ctx),
		c.elastic.Search.WithIndex(c.txIndex),
		c.elastic.Search.WithBody(&query),
	)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return TransactionsSearchResponse{}, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Decode the response into a map.
	var result TransactionsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("decoding response: %w", err)
	}

	return result, nil
}

func createIdentitiesQuery(ID string, pageSize, pageNumber int, desc bool, startTick, endTick uint32) (bytes.Buffer, error) {
	from := pageNumber * pageSize
	querySort := "asc"
	if desc {
		querySort = "desc"
	}

	tickNumberRangeFilter := map[string]interface{}{
		"lte": endTick,
		"gte": startTick,
	}
	if endTick <= 0 {
		delete(tickNumberRangeFilter, "lte")
	}

	query := map[string]interface{}{
		"track_total_hits": "10000", // limit to max page size for better performance
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []interface{}{
					map[string]interface{}{
						"match": map[string]interface{}{
							"source": ID,
						},
					},
					map[string]interface{}{
						"match": map[string]interface{}{
							"destination": ID,
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

type TxHit struct {
	Source Tx `json:"_source"`
}

type TransactionsSearchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []TxHit `json:"hits"`
	} `json:"hits"`
}

type TransactionGetResponse struct {
	Index       string `json:"_index"`
	Id          string `json:"_id"`
	Version     int    `json:"_version"`
	SeqNo       int    `json:"_seq_no"`
	PrimaryTerm int    `json:"_primary_term"`
	Found       bool   `json:"found"`
	Source      Tx     `json:"_source"`
}

type Tx struct {
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
