package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
)

func performElasticSearch(ctx context.Context, esClient *elasticsearch.Client, index string, query io.Reader, result any) error {
	res, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(index),
		esClient.Search.WithBody(query),
	)
	if err != nil {
		log.Printf("[DEBUG] calling es client search with query: %s", query)
		return fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("error response from data store: %s", res.String())
	}

	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}

	return nil
}
