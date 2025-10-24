package elastic

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/elastic/go-elasticsearch/v8"
)

var ErrDocumentNotFound = errors.New("document not found")

type SearchClient interface {
	QueryTickDataByTickNumber(ctx context.Context, tickNumber uint32) (TickDataGetResponse, error)
	QueryEmptyTicks(ctx context.Context, startTick, endTick, epoch uint32) ([]uint32, error)
	QueryComputorListByEpoch(ctx context.Context, epoch uint32) (ComputorsListSearchResponse, error)
	QueryIdentityTransactions(ctx context.Context, ID string, pageSize, pageNumber int, desc bool, startTick, endTick uint32) (TransactionsSearchResponse, error)
	QueryTransactionByHash(ctx context.Context, txID string) (TransactionGetResponse, error)
	QueryTickTransactions(ctx context.Context, tick uint32) (TransactionsSearchResponse, error)
}

type Client struct {
	elastic                      *elasticsearch.Client
	ConsecutiveElasticErrorCount atomic.Int32
	TotalElasticErrorCount       atomic.Int32
	txIndex                      string
	tickDataIndex                string
	computorListIndex            string
}

func NewElasticClient(txIndex, tickDataIndex, clIndex string, esClient *elasticsearch.Client) *Client {
	return &Client{
		txIndex:           txIndex,
		tickDataIndex:     tickDataIndex,
		elastic:           esClient,
		computorListIndex: clIndex,
	}
}
