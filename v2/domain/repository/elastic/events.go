package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
)

type EventsRepository struct {
	esClient   *elasticsearch.Client
	eventIndex string
}

func NewEventsRepository(eventIndex string, esClient *elasticsearch.Client) *EventsRepository {
	return &EventsRepository{
		eventIndex: eventIndex,
		esClient:   esClient,
	}
}

type event struct {
	Epoch                  uint32 `json:"epoch"`
	TickNumber             uint32 `json:"tickNumber"`
	Timestamp              uint64 `json:"timestamp"`
	EmittingContractIndex  uint64 `json:"emittingContractIndex"`
	TransactionHash        string `json:"transactionHash"`
	LogID                  uint64 `json:"logId"`
	LogDigest              string `json:"logDigest"`
	Type                   uint32 `json:"type"`
	Category               uint32 `json:"category"`
	Source                 string `json:"source"`
	Destination            string `json:"destination"`
	Amount                 uint64 `json:"amount"`
	AssetName              string `json:"assetName"`
	AssetIssuer            string `json:"assetIssuer"`
	NumberOfShares         uint64 `json:"numberOfShares"`
	ManagingContractIndex  uint64 `json:"managingContractIndex"`
	UnitOfMeasurement      string `json:"unitOfMeasurement"`
	NumberOfDecimalPlaces  uint32 `json:"numberOfDecimalPlaces"`
	DeductedAmount         uint64 `json:"deductedAmount"`
	RemainingAmount        int64  `json:"remainingAmount"`
	ContractIndex          uint64 `json:"contractIndex"`
	ContractIndexBurnedFor uint64 `json:"contractIndexBurnedFor"`
}

type eventHit struct {
	Source event `json:"_source"`
}

type eventsSearchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []eventHit `json:"hits"`
	} `json:"hits"`
}

func (r *EventsRepository) GetEvents(ctx context.Context, filters map[string][]string, from, size uint32) ([]*api.Event, *entities.Hits, error) {
	query := createEventsQuery(filters, from, size)

	res, err := r.esClient.Search(
		r.esClient.Search.WithContext(ctx),
		r.esClient.Search.WithIndex(r.eventIndex),
		r.esClient.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("performing search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, nil, fmt.Errorf("error response from data store: %s", res.String())
	}

	var result eventsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, nil, fmt.Errorf("decoding response: %w", err)
	}

	hits := &entities.Hits{
		Total:    result.Hits.Total.Value,
		Relation: result.Hits.Total.Relation,
	}

	return eventHitsToAPIEvents(result.Hits.Hits), hits, nil
}

func createEventsQuery(filters map[string][]string, from, size uint32) string {
	filterStrings := make([]string, 0, len(filters))

	keys := getSortedKeys(filters)
	for _, k := range keys {
		esField := k
		if k == "eventType" {
			esField = "type"
		}
		if len(filters[k]) == 1 {
			filterStrings = append(filterStrings, fmt.Sprintf(`{"term":{"%s":"%s"}}`, esField, filters[k][0]))
		}
	}

	filterClause := ""
	if len(filterStrings) > 0 {
		filterClause = strings.Join(filterStrings, ",")
	}

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf(`{
		"query": {
			"bool": {
				"filter": [%s]
			}
		},
		"sort": [{"tickNumber":{"order":"desc"}}],
		"from": %d,
		"size": %d,
		"track_total_hits": %d
	}`, filterClause, from, size, maxTrackTotalHits))

	return buf.String()
}
