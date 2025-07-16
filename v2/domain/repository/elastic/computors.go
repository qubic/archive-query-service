package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
)

type computorsList struct {
	Epoch      uint32   `json:"epoch"`
	TickNumber uint32   `json:"tickNumber"`
	Identities []string `json:"identities"`
	Signature  string   `json:"signature"`
}

type computorsListHit struct {
	Source computorsList `json:"_source"`
}

type computorsListSearchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []computorsListHit `json:"hits"`
	} `json:"hits"`
}

func (r *Repository) GetComputorsListsForEpoch(ctx context.Context, epoch uint32) ([]*api.ComputorsList, error) {

	query, err := createComputorsListQuery(epoch)
	if err != nil {
		return nil, fmt.Errorf("creating query %w", err)
	}

	res, err := r.esClient.Search(
		r.esClient.Search.WithContext(ctx),
		r.esClient.Search.WithIndex(r.clIndex),
		r.esClient.Search.WithBody(&query),
	)
	if err != nil {
		return nil, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("got error response from data store: %s", res.String())
	}

	var result computorsListSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return computorsListHitsToAPIObjects(result.Hits.Hits), nil

}

func createComputorsListQuery(epoch uint32) (bytes.Buffer, error) {
	query := map[string]interface{}{
		"track_total_hits": "true",
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"epoch": epoch,
			},
		},
		"sort": map[string]interface{}{
			"tickNumber": "desc",
		},
		"size": 100,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return bytes.Buffer{}, fmt.Errorf("encoding query: %w", err)
	}
	return buf, nil
}
