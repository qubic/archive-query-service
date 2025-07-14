package elastic

import (
	"github.com/elastic/go-elasticsearch/v8"
	"sync/atomic"
)

type Repository struct {
	esClient                     *elasticsearch.Client
	ConsecutiveElasticErrorCount atomic.Int32
	TotalElasticErrorCount       atomic.Int32
	txIndex                      string
	tickDataIndex                string
	clIndex                      string
}

func NewRepository(txIndex, tickDataIndex, clIndex string, esClient *elasticsearch.Client) *Repository {
	return &Repository{
		txIndex:       txIndex,
		tickDataIndex: tickDataIndex,
		esClient:      esClient,
		clIndex:       clIndex,
	}
}
