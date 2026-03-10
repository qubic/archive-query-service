package elastic

import (
	"sync/atomic"

	"github.com/elastic/go-elasticsearch/v8"
)

type ArchiveRepository struct {
	esClient                     *elasticsearch.Client
	ConsecutiveElasticErrorCount atomic.Int32
	TotalElasticErrorCount       atomic.Int32
	txIndex                      string
	tickDataIndex                string
	clIndex                      string
}

func NewArchiveRepository(txIndex, tickDataIndex, clIndex string, esClient *elasticsearch.Client) *ArchiveRepository {
	return &ArchiveRepository{
		txIndex:       txIndex,
		tickDataIndex: tickDataIndex,
		esClient:      esClient,
		clIndex:       clIndex,
	}
}
