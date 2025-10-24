package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/qubic/archive-query-service/elastic"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
)

var ErrDocumentNotFound = errors.New("document not found")

type QueryCache interface {
	GetMaxTick(ctx context.Context) (uint32, error)
	GetTickIntervals(ctx context.Context) ([]*statusPb.TickInterval, error)
	GetEmptyTicks(epoch uint32) *EmptyTicks
	SetEmptyTicks(ticks *EmptyTicks)
}

type ElasticClient interface {
}

type QueryService struct {
	elasticClient                elastic.SearchClient
	ConsecutiveElasticErrorCount atomic.Int32
	TotalElasticErrorCount       atomic.Int32
	cache                        QueryCache
	txIndex                      string
	tickDataIndex                string
	computorListIndex            string
	emptyTicksLock               sync.Mutex
}

func NewQueryService(txIndex, tickDataIndex, computorListIndex string, elasticClient elastic.SearchClient, cache QueryCache) *QueryService {
	return &QueryService{
		elasticClient:     elasticClient,
		txIndex:           txIndex,
		tickDataIndex:     tickDataIndex,
		computorListIndex: computorListIndex,
		cache:             cache,
	}
}

func (qs *QueryService) performGetTickDataByTickNumberQuery(ctx context.Context, tickNumber uint32) (result elastic.TickDataGetResponse, err error) {
	res, err := qs.elasticClient.QueryTickDataByTickNumber(ctx, tickNumber)
	if err != nil && !errors.Is(err, ErrDocumentNotFound) {
		qs.TotalElasticErrorCount.Add(1)
		qs.ConsecutiveElasticErrorCount.Add(1)
	} else {
		qs.ConsecutiveElasticErrorCount.Store(0)
	}
	return res, err
}

func (qs *QueryService) performIdentitiesTransactionsQuery(ctx context.Context, ID string, pageSize, pageNumber int, desc bool, reqStartTick, reqEndTick uint32) (elastic.TransactionsSearchResponse, error) {
	statusMaxTick, err := qs.cache.GetMaxTick(ctx)
	if err != nil {
		return elastic.TransactionsSearchResponse{}, fmt.Errorf("getting max tick from cache: %w", err)
	}

	var queryEndTick uint32
	if reqEndTick != 0 && reqEndTick <= statusMaxTick {
		queryEndTick = reqEndTick
	} else {
		queryEndTick = statusMaxTick
	}

	res, err := qs.elasticClient.QueryIdentityTransactions(ctx, ID, pageSize, pageNumber, desc, reqStartTick, queryEndTick)
	if err != nil {
		qs.TotalElasticErrorCount.Add(1)
		qs.ConsecutiveElasticErrorCount.Add(1)
	} else {
		qs.ConsecutiveElasticErrorCount.Store(0)
	}
	return res, err

}

func (qs *QueryService) performGetTxByIDQuery(ctx context.Context, txID string) (elastic.TransactionGetResponse, error) {
	res, err := qs.elasticClient.QueryTransactionByHash(ctx, txID)
	if err != nil && !errors.Is(err, elastic.ErrDocumentNotFound) {
		qs.TotalElasticErrorCount.Add(1)
		qs.ConsecutiveElasticErrorCount.Add(1)
	} else {
		qs.ConsecutiveElasticErrorCount.Store(0)
	}
	return res, err
}

func (qs *QueryService) performTickTransactionsQuery(ctx context.Context, tick uint32) (elastic.TransactionsSearchResponse, error) {

	res, err := qs.elasticClient.QueryTickTransactions(ctx, tick)
	if err != nil {
		qs.TotalElasticErrorCount.Add(1)
		qs.ConsecutiveElasticErrorCount.Add(1)
	} else {
		qs.ConsecutiveElasticErrorCount.Store(0)
	}
	return res, err
}

func (qs *QueryService) performComputorListByEpochQuery(ctx context.Context, epoch uint32) (result elastic.ComputorsListSearchResponse, err error) {
	computors, err := qs.elasticClient.QueryComputorListByEpoch(ctx, epoch)
	if err != nil {
		qs.TotalElasticErrorCount.Add(1)
		qs.ConsecutiveElasticErrorCount.Add(1)
	} else {
		qs.ConsecutiveElasticErrorCount.Store(0)
	}
	return computors, err
}
