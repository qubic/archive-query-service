package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

func (c *Client) QueryComputorListByEpoch(ctx context.Context, epoch uint32) (result ComputorsListSearchResponse, err error) {

	query, err := createComputorsListQuery(epoch)
	if err != nil {
		return ComputorsListSearchResponse{}, fmt.Errorf("creating query: %w", err)
	}

	res, err := c.elastic.Search(
		c.elastic.Search.WithContext(ctx),
		c.elastic.Search.WithIndex(c.computorListIndex),
		c.elastic.Search.WithBody(&query),
	)
	if err != nil {
		return ComputorsListSearchResponse{}, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return ComputorsListSearchResponse{}, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return ComputorsListSearchResponse{}, fmt.Errorf("decoding response: %w", err)
	}

	return result, nil
}

func createComputorsListQuery(epoch uint32) (bytes.Buffer, error) {
	query := map[string]interface{}{
		"track_total_hits": "10000",
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

type computorsListHit struct {
	Source ComputorsList `json:"_source"`
}

type ComputorsListSearchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []computorsListHit `json:"hits"`
	} `json:"hits"`
}

type ComputorsList struct {
	Epoch      uint32   `json:"epoch"`
	TickNumber uint32   `json:"tickNumber"`
	Identities []string `json:"identities"`
	Signature  string   `json:"signature"`
}
