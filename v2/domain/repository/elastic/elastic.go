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
}

func NewRepository(txIndex string, tickDataIndex string, esClient *elasticsearch.Client) *Repository {
	return &Repository{
		txIndex:       txIndex,
		tickDataIndex: tickDataIndex,
		esClient:      esClient,
	}
}
