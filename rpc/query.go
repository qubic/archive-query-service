package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jellydator/ttlcache/v3"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"sync/atomic"
)

type QueryBuilder struct {
	esClient                     *elasticsearch.Client
	ConsecutiveElasticErrorCount atomic.Int32
	TotalElasticErrorCount       atomic.Int32
	StatusServiceClient          statusPb.StatusServiceClient
	cache                        *ttlcache.Cache[string, uint32]
}

func NewQueryBuilder(esClient *elasticsearch.Client, statusServiceClient statusPb.StatusServiceClient, cache *ttlcache.Cache[string, uint32]) *QueryBuilder {
	return &QueryBuilder{esClient: esClient, StatusServiceClient: statusServiceClient, cache: cache}
}

func (qb *QueryBuilder) fetchStatusMaxTick(ctx context.Context) (uint32, error) {
	statusResponse, err := qb.StatusServiceClient.GetStatus(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("fetching status service: %v", err)
	}

	return statusResponse.LastProcessedTick, nil
}

func (qb *QueryBuilder) performIdentitiesTransactionsQuery(ctx context.Context, ID string, pageSize, pageNumber int, desc bool) (TransactionsSearchResponse, error) {

	var maxTick uint32
	if qb.cache.Has(MaxTickCacheKey) {
		item := qb.cache.Get(MaxTickCacheKey)
		maxTick = item.Value()
	} else {

		httpMaxTick, err := qb.fetchStatusMaxTick(ctx)
		if err != nil {
			return TransactionsSearchResponse{}, fmt.Errorf("fetching status service max tick: %v", err)
		}

		qb.cache.Set(MaxTickCacheKey, httpMaxTick, ttlcache.DefaultTTL)
		item := qb.cache.Get(MaxTickCacheKey)
		maxTick = item.Value()
	}

	query, err := createIdentitiesQuery(ID, pageSize, pageNumber, desc, maxTick)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("creating query: %v", err)
	}

	res, err := qb.esClient.Search(
		qb.esClient.Search.WithContext(ctx),
		qb.esClient.Search.WithIndex("qubic-transactions-alias"),
		qb.esClient.Search.WithBody(&query),
		qb.esClient.Search.WithPretty(),
	)
	if err != nil {
		qb.TotalElasticErrorCount.Add(1)
		qb.ConsecutiveElasticErrorCount.Add(1)
		return TransactionsSearchResponse{}, fmt.Errorf("performing search: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		qb.TotalElasticErrorCount.Add(1)
		qb.ConsecutiveElasticErrorCount.Add(1)
		return TransactionsSearchResponse{}, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Decode the response into a map.
	var result TransactionsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("decoding response: %v", err)
	}

	qb.ConsecutiveElasticErrorCount.Store(0)

	return result, nil
}

func (qb *QueryBuilder) performTickTransactionsQuery(ctx context.Context, tick uint32) (TransactionsSearchResponse, error) {
	query, err := createTickTransactionsQuery(tick)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("creating query: %v", err)
	}

	res, err := qb.esClient.Search(
		qb.esClient.Search.WithContext(ctx),
		qb.esClient.Search.WithIndex("qubic-transactions-alias"),
		qb.esClient.Search.WithBody(&query),
		qb.esClient.Search.WithPretty(),
	)
	if err != nil {
		qb.TotalElasticErrorCount.Add(1)
		qb.ConsecutiveElasticErrorCount.Add(1)
		return TransactionsSearchResponse{}, fmt.Errorf("performing search: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		qb.TotalElasticErrorCount.Add(1)
		qb.ConsecutiveElasticErrorCount.Add(1)
		return TransactionsSearchResponse{}, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Decode the response into a map.
	var result TransactionsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("decoding response: %v", err)
	}

	qb.ConsecutiveElasticErrorCount.Store(0)

	return result, nil
}

func createIdentitiesQuery(ID string, pageSize, pageNumber int, desc bool, maxTick uint32) (bytes.Buffer, error) {
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
		"track_total_hits": "true",
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
		return bytes.Buffer{}, fmt.Errorf("encoding query: %v", err)
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
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return bytes.Buffer{}, fmt.Errorf("encoding query: %v", err)
	}

	return buf, nil
}

type TransactionsSearchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			Source Tx `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
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
