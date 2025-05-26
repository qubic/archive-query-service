package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jellydator/ttlcache/v3"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"strconv"
	"sync/atomic"
)

type QueryBuilder struct {
	esClient                     *elasticsearch.Client
	ConsecutiveElasticErrorCount atomic.Int32
	TotalElasticErrorCount       atomic.Int32
	StatusServiceClient          statusPb.StatusServiceClient
	cache                        *ttlcache.Cache[string, uint32]
	txIndex                      string
	tickDataIndex                string
}

func NewQueryBuilder(txIndex string, tickDataIndex string, esClient *elasticsearch.Client, statusServiceClient statusPb.StatusServiceClient, cache *ttlcache.Cache[string, uint32]) *QueryBuilder {
	return &QueryBuilder{
		txIndex:             txIndex,
		tickDataIndex:       tickDataIndex,
		esClient:            esClient,
		StatusServiceClient: statusServiceClient,
		cache:               cache,
	}
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
		qb.esClient.Search.WithIndex(qb.txIndex),
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

func (qb *QueryBuilder) performGetTxByIDQuery(ctx context.Context, txID string) (TransactionGetResponse, error) {
	res, err := qb.esClient.Get(qb.txIndex, txID)
	if err != nil {
		return TransactionGetResponse{}, fmt.Errorf("calling es client get: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return TransactionGetResponse{}, fmt.Errorf("got error response from Elasticsearch: %s", res.String())
	}

	var result TransactionGetResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return TransactionGetResponse{}, fmt.Errorf("decoding response: %v", err)
	}

	return result, nil
}

func (qb *QueryBuilder) performGetTickDataByTickNumberQuery(ctx context.Context, tickNumber uint32) (TickDataGetResponse, error) {
	res, err := qb.esClient.Get(qb.tickDataIndex, strconv.FormatUint(uint64(tickNumber), 10))
	if err != nil {
		return TickDataGetResponse{}, fmt.Errorf("calling es client get: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return TickDataGetResponse{}, fmt.Errorf("got error response from Elasticsearch: %s", res.String())
	}

	var result TickDataGetResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return TickDataGetResponse{}, fmt.Errorf("decoding response: %v", err)
	}

	return result, nil
}

func (qb *QueryBuilder) performTickTransactionsQuery(ctx context.Context, tick uint32) (TransactionsSearchResponse, error) {
	query, err := createTickTransactionsQuery(tick)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("creating query: %v", err)
	}

	res, err := qb.esClient.Search(
		qb.esClient.Search.WithContext(ctx),
		qb.esClient.Search.WithIndex(qb.txIndex),
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

type TickDataGetResponse struct {
	Index       string   `json:"_index"`
	Id          string   `json:"_id"`
	Version     int      `json:"_version"`
	SeqNo       int      `json:"_seq_no"`
	PrimaryTerm int      `json:"_primary_term"`
	Found       bool     `json:"found"`
	Source      TickData `json:"_source"`
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

type TickData struct {
	ComputorIndex     uint32   `json:"computorIndex"`
	Epoch             uint32   `json:"epoch"`
	TickNumber        uint32   `json:"tickNumber"`
	Timestamp         uint64   `json:"timestamp"`
	VarStruct         string   `json:"varStruct"`
	Timelock          string   `json:"timelock"`
	TransactionHashes []string `json:"transactionHashes"`
	ContractFees      []int64  `json:"contractFees"`
	Signature         string   `json:"signature"`
}
