package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/domain"
	"github.com/qubic/archive-query-service/v2/entities"
	"log"
	"sort"
	"strings"
)

type transactionGetResponse struct {
	Index       string      `json:"_index"`
	Id          string      `json:"_id"`
	Version     int         `json:"_version"`
	SeqNo       int         `json:"_seq_no"`
	PrimaryTerm int         `json:"_primary_term"`
	Found       bool        `json:"found"`
	Source      transaction `json:"_source"`
}

type transactionHit struct {
	Source transaction `json:"_source"`
}

type transactionsSearchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []transactionHit `json:"hits"`
	} `json:"hits"`
}

type transaction struct {
	Hash        string `json:"hash"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      uint64 `json:"amount"`
	TickNumber  uint32 `json:"tickNumber"`
	InputType   uint32 `json:"inputType"`
	InputSize   uint32 `json:"inputSize"`
	InputData   string `json:"inputData"`
	Signature   string `json:"signature"`
	Timestamp   uint64 `json:"timestamp"`
	MoneyFlew   bool   `json:"moneyFlew"`
}

const maxTrackTotalHits int = 10000 // limit for better performance

func (r *Repository) GetTransactionByHash(ctx context.Context, hash string) (*api.Transaction, error) {
	res, err := r.esClient.Get(r.txIndex, hash)
	if err != nil {
		return nil, fmt.Errorf("calling es client get with: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, domain.ErrNotFound
	}

	if res.IsError() {
		return nil, fmt.Errorf("got error response from data store: %s", res.String())
	}

	var result transactionGetResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding json response: %w", err)
	}

	return transactionToApiTransaction(result.Source), nil
}

func (r *Repository) GetTransactionsForTickNumber(ctx context.Context, tickNumber uint32) ([]*api.Transaction, error) {
	query, err := createTickTransactionsQuery(tickNumber)
	if err != nil {
		return nil, fmt.Errorf("creating query: %w", err)
	}

	res, err := r.esClient.Search(
		r.esClient.Search.WithContext(ctx),
		r.esClient.Search.WithIndex(r.txIndex),
		r.esClient.Search.WithBody(&query),
		r.esClient.Search.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("got error response from data store: %s", res.String())
	}

	var result transactionsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return transactionHitsToApiTransactions(result.Hits.Hits), nil
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

func (r *Repository) GetTransactionsForIdentity(ctx context.Context, identity string, maxTick uint32, filters map[string]string, ranges map[string][]*entities.Range, from, size uint32) ([]*api.Transaction, *entities.Hits, error) {
	query, err := createIdentitiesQueryString(identity, filters, ranges, from, size, maxTick)
	if err != nil {
		return nil, nil, fmt.Errorf("creating query: %w", err)
	}

	res, err := r.esClient.Search(
		r.esClient.Search.WithContext(ctx),
		r.esClient.Search.WithIndex(r.txIndex),
		r.esClient.Search.WithBody(strings.NewReader(query)),
		r.esClient.Search.WithPretty(),
	)
	if err != nil {
		log.Printf("calling es client search with query: %v", query)
		return nil, nil, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, nil, fmt.Errorf("error response from data store: %s", res.String())
	}

	var result transactionsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, nil, fmt.Errorf("decoding response: %w", err)
	}

	hits := &entities.Hits{
		Total:    result.Hits.Total.Value,
		Relation: result.Hits.Total.Relation,
	}

	return transactionHitsToApiTransactions(result.Hits.Hits), hits, nil
}

func createIdentitiesQueryString(identity string, filters map[string]string, ranges map[string][]*entities.Range, from, size, maxTick uint32) (string, error) {
	var query string

	filterStrings := make([]string, 0, len(filters)+len(ranges)+1)

	// restrict to max tick (we don't care about potential duplicate tickNumber range filter)
	filterStrings = append(filterStrings, fmt.Sprintf(`{"range":{"tickNumber":{"lte":"%d"}}}`, maxTick))
	filterStrings = append(filterStrings, getFilterStrings(filters)...)
	rangeFilterStrings, err := getRangeFilterStrings(ranges)
	if err != nil {
		return "", err
	}
	filterStrings = append(filterStrings, rangeFilterStrings...)

	// in case we have source or destination filter the should clause still works
	query = `{ 
      "query": {
		"bool": {
		  "should": [
			{ "term":{"source":"%s"} },
			{ "term":{"destination":"%s"} }
		  ],
		  "minimum_should_match": 1,
		  "filter": [ %s ]
		}
	  },
	  "sort": [ {"tickNumber":{"order":"desc"}} ],
	  "from": %d,
	  "size": %d,
	  "track_total_hits": %d
	}`

	query = fmt.Sprintf(query, identity, identity, strings.Join(filterStrings, ","), from, size, maxTrackTotalHits)
	return query, nil
}

func getFilterStrings(filters map[string]string) []string {
	keys := getSortedKeys(filters) // sort for a deterministic filter order

	filterStrings := make([]string, 0, len(filters))
	for _, k := range keys {
		filterStrings = append(filterStrings, fmt.Sprintf(`{"term":{"%s":"%s"}}`, k, filters[k]))
	}
	return filterStrings
}

func getRangeFilterStrings(ranges map[string][]*entities.Range) ([]string, error) {
	keys := getSortedKeys(ranges) // sort for a deterministic filter order
	filterStrings := make([]string, 0, len(ranges))
	for _, k := range keys {
		rangeString, err := createRangeFilter(k, ranges[k])
		if err != nil {
			log.Printf("error computing range filter [%s]: %v", k, ranges[k])
			return nil, fmt.Errorf("creating range filter: %w", err)
		}
		filterStrings = append(filterStrings, rangeString)
	}
	return filterStrings, nil
}

func createRangeFilter(property string, r []*entities.Range) (string, error) {
	var rangeStrings []string
	for _, v := range r {
		rangeStrings = append(rangeStrings, fmt.Sprintf(`"%s":"%s"`, v.Operation, v.Value))
	}
	if len(rangeStrings) > 0 {
		return fmt.Sprintf(`{"range":{"%s":{%s}}}`, property, strings.Join(rangeStrings, ",")), nil
	} else {
		return "", fmt.Errorf("computing range for [%s]", property)
	}
}

func getSortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
