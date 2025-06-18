package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/jellydator/ttlcache/v3"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"strconv"
	"sync/atomic"
	"time"
)

var ErrDocumentNotFound = errors.New("document not found")

const maxTickCacheKey = "max_tick"
const tickIntervalsCacheKey = "tick_intervals"

type StatusCache struct {
	lastProcessedTickProvider *ttlcache.Cache[string, uint32]
	tickIntervalsProvider     *ttlcache.Cache[string, []*statusPb.TickInterval]
	StatusServiceClient       statusPb.StatusServiceClient
}

func NewStatusCache(statusServiceClient statusPb.StatusServiceClient, ttl time.Duration) *StatusCache {
	lastProcessedTickProvider := ttlcache.New[string, uint32](
		ttlcache.WithTTL[string, uint32](ttl),
		ttlcache.WithDisableTouchOnHit[string, uint32](), // don't refresh ttl upon getting the item from cache
	)

	tickIntervalsProvider := ttlcache.New[string, []*statusPb.TickInterval](
		ttlcache.WithTTL[string, []*statusPb.TickInterval](ttl),
		ttlcache.WithDisableTouchOnHit[string, []*statusPb.TickInterval](), // don't refresh ttl upon getting the item from cache
	)
	return &StatusCache{
		lastProcessedTickProvider: lastProcessedTickProvider,
		tickIntervalsProvider:     tickIntervalsProvider,
		StatusServiceClient:       statusServiceClient,
	}
}

func (s *StatusCache) GetMaxTick(ctx context.Context) (uint32, error) {
	if s.lastProcessedTickProvider.Has(maxTickCacheKey) {
		item := s.lastProcessedTickProvider.Get(maxTickCacheKey)
		if item != nil {
			return item.Value(), nil
		}
	}

	maxTick, err := s.fetchStatusMaxTick(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetching status service max tick: %w", err)
	}

	s.lastProcessedTickProvider.Set(maxTickCacheKey, maxTick, ttlcache.DefaultTTL)
	return maxTick, nil
}

func (s *StatusCache) GetTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	if s.tickIntervalsProvider.Has(tickIntervalsCacheKey) {
		item := s.tickIntervalsProvider.Get(tickIntervalsCacheKey)
		if item != nil {
			return item.Value(), nil
		}
	}

	tickIntervals, err := s.fetchTickIntervals(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching status service max tick: %w", err)
	}

	s.tickIntervalsProvider.Set(tickIntervalsCacheKey, tickIntervals, ttlcache.DefaultTTL)
	return tickIntervals, nil
}

func (s *StatusCache) Start() {
	s.lastProcessedTickProvider.Start()
	s.tickIntervalsProvider.Start()
}

func (s *StatusCache) Stop() {
	s.lastProcessedTickProvider.Stop()
	s.tickIntervalsProvider.Stop()
}

type QueryBuilder struct {
	esClient                     *elasticsearch.Client
	ConsecutiveElasticErrorCount atomic.Int32
	TotalElasticErrorCount       atomic.Int32
	cache                        *StatusCache
	txIndex                      string
	tickDataIndex                string
}

func NewQueryBuilder(txIndex string, tickDataIndex string, esClient *elasticsearch.Client, cache *StatusCache) *QueryBuilder {
	return &QueryBuilder{
		txIndex:       txIndex,
		tickDataIndex: tickDataIndex,
		esClient:      esClient,
		cache:         cache,
	}
}

func (s *StatusCache) fetchStatusMaxTick(ctx context.Context) (uint32, error) {
	statusResponse, err := s.StatusServiceClient.GetStatus(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("fetching status service: %w", err)
	}

	return statusResponse.LastProcessedTick, nil
}

func (s *StatusCache) fetchTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error) {
	tickIntervalsResponse, err := s.StatusServiceClient.GetTickIntervals(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("fetching tick intervals: %w", err)
	}

	if len(tickIntervalsResponse.Intervals) == 0 {
		return nil, fmt.Errorf("no tick intervals found")
	}

	return tickIntervalsResponse.Intervals, nil
}

func (qb *QueryBuilder) performIdentitiesTransactionsQuery(ctx context.Context, ID string, pageSize, pageNumber int, desc bool, reqStartTick, reqEndTick uint32) (result TransactionsSearchResponse, err error) {
	statusMaxTick, err := qb.cache.GetMaxTick(ctx)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("getting max tick from cache: %w", err)
	}

	var queryEndTick uint32
	if reqEndTick != 0 && reqEndTick <= statusMaxTick {
		queryEndTick = reqEndTick
	} else {
		queryEndTick = statusMaxTick
	}

	query, err := createIdentitiesQuery(ID, pageSize, pageNumber, desc, reqStartTick, queryEndTick)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("creating query: %w", err)
	}

	defer func() {
		if err != nil {
			qb.TotalElasticErrorCount.Add(1)
			qb.ConsecutiveElasticErrorCount.Add(1)
		} else {
			qb.ConsecutiveElasticErrorCount.Store(0)
		}
	}()

	res, err := qb.esClient.Search(
		qb.esClient.Search.WithContext(ctx),
		qb.esClient.Search.WithIndex(qb.txIndex),
		qb.esClient.Search.WithBody(&query),
		qb.esClient.Search.WithPretty(),
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

func (qb *QueryBuilder) performGetTxByIDQuery(_ context.Context, txID string) (result TransactionGetResponse, err error) {
	defer func() {
		if errors.Is(err, ErrDocumentNotFound) {
			return
		}

		if err != nil {
			qb.TotalElasticErrorCount.Add(1)
			qb.ConsecutiveElasticErrorCount.Add(1)
		} else {
			qb.ConsecutiveElasticErrorCount.Store(0)
		}
	}()

	res, err := qb.esClient.Get(qb.txIndex, txID)
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

func (qb *QueryBuilder) performGetTickDataByTickNumberQuery(_ context.Context, tickNumber uint32) (result TickDataGetResponse, err error) {
	defer func() {
		if errors.Is(err, ErrDocumentNotFound) {
			return
		}

		if err != nil {
			qb.TotalElasticErrorCount.Add(1)
			qb.ConsecutiveElasticErrorCount.Add(1)
		} else {
			qb.ConsecutiveElasticErrorCount.Store(0)
		}
	}()

	res, err := qb.esClient.Get(qb.tickDataIndex, strconv.FormatUint(uint64(tickNumber), 10))
	if err != nil {
		return TickDataGetResponse{}, fmt.Errorf("calling es client get: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return TickDataGetResponse{}, ErrDocumentNotFound
	}

	if res.IsError() {
		return TickDataGetResponse{}, fmt.Errorf("got error response from Elasticsearch: %s", res.String())
	}

	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return TickDataGetResponse{}, fmt.Errorf("decoding response: %w", err)
	}

	return result, nil
}

func (qb *QueryBuilder) performTickTransactionsQuery(ctx context.Context, tick uint32) (TransactionsSearchResponse, error) {
	query, err := createTickTransactionsQuery(tick)
	if err != nil {
		return TransactionsSearchResponse{}, fmt.Errorf("creating query: %w", err)
	}

	defer func() {
		if err != nil {
			qb.TotalElasticErrorCount.Add(1)
			qb.ConsecutiveElasticErrorCount.Add(1)
		} else {
			qb.ConsecutiveElasticErrorCount.Store(0)
		}
	}()

	res, err := qb.esClient.Search(
		qb.esClient.Search.WithContext(ctx),
		qb.esClient.Search.WithIndex(qb.txIndex),
		qb.esClient.Search.WithBody(&query),
		qb.esClient.Search.WithPretty(),
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
