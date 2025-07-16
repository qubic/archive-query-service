package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

func (r *Repository) performElasticSearch(ctx context.Context, index string, query *bytes.Buffer, result any) error {
	res, err := r.esClient.Search(
		r.esClient.Search.WithContext(ctx),
		r.esClient.Search.WithIndex(index),
		r.esClient.Search.WithBody(query),
	)
	if err != nil {
		return fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("got error response from data store: %s", res.String())
	}

	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}

	return nil
}
