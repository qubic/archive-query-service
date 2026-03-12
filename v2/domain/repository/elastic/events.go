package elastic

import (
	"context"
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
	Epoch                    uint32  `json:"epoch"`
	TickNumber               uint32  `json:"tickNumber"`
	Timestamp                uint64  `json:"timestamp"`
	TransactionHash          *string `json:"transactionHash"` // not all events belong to a transaction
	LogID                    uint64  `json:"logId"`
	LogDigest                string  `json:"logDigest"`
	LogType                  uint32  `json:"logType"`
	Categories               []int32 `json:"categories"` // not all events have categories
	Source                   string  `json:"source"`
	Destination              string  `json:"destination"`
	Amount                   uint64  `json:"amount"`
	AssetName                string  `json:"assetName"`
	AssetIssuer              string  `json:"assetIssuer"`
	NumberOfShares           uint64  `json:"numberOfShares"`
	ManagingContractIndex    uint64  `json:"managingContractIndex"`
	UnitOfMeasurement        string  `json:"unitOfMeasurement"`
	NumberOfDecimalPlaces    uint32  `json:"numberOfDecimalPlaces"`
	DeductedAmount           uint64  `json:"deductedAmount"`
	RemainingAmount          int64   `json:"remainingAmount"`
	ContractIndex            uint64  `json:"contractIndex"`
	Possessor                string  `json:"possessor"`
	Owner                    string  `json:"owner"`
	SourceContractIndex      uint64  `json:"sourceContractIndex"`
	DestinationContractIndex uint64  `json:"destinationContractIndex"`
	CustomMessage            uint64  `json:"customMessage"`
	ContractMessageType      uint64  `json:"contractMessageType"`
	RawPayload               []byte  `json:"rawPayload"` // not all events have raw payload
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

func (r *EventsRepository) GetEvents(ctx context.Context, filters entities.Filters, from, size uint32) ([]*api.Event, *entities.Hits, error) {
	query, err := createEventsQuery(filters, from, size)
	if err != nil {
		return nil, nil, fmt.Errorf("creating events query: %w", err)
	}

	var result eventsSearchResponse
	err = performElasticSearch(ctx, r.esClient, r.eventIndex, strings.NewReader(query), &result)
	if err != nil {
		return nil, nil, fmt.Errorf("performing elastic search: %w", err)
	}

	hits := &entities.Hits{
		Total:    result.Hits.Total.Value,
		Relation: result.Hits.Total.Relation,
	}

	return eventHitsToAPIEvents(result.Hits.Hits), hits, nil
}

func createEventsQuery(filters entities.Filters, from, size uint32) (string, error) {
	filterStrings := make([]string, 0, len(filters.Include))

	// append include filters to filter section
	filterStrings = append(filterStrings, getFilterStrings(filters.Include)...)

	// append range filters to filter section
	rangeFilterStrings, err := getRangeFilterStrings(filters.Ranges)
	if err != nil {
		return "", err
	}
	filterStrings = append(filterStrings, rangeFilterStrings...)

	// append should filters to filter section
	shouldFilterStrings, err := getShouldFilterStrings(filters.Should)
	if err != nil {
		return "", fmt.Errorf("creating should filters: %w", err)
	}
	filterStrings = append(filterStrings, shouldFilterStrings...)

	// exclude filters
	excludeFilterStrings := getFilterStrings(filters.Exclude)

	// empty bool query clause
	boolClause := make([]string, 0, 2)

	// append include filters if not empty
	filterClause := strings.Join(filterStrings, ",")
	if len(filterClause) > 0 {
		filterClause = fmt.Sprintf(`"filter": [%s]`, filterClause)
		boolClause = append(boolClause, filterClause)
	}

	// append exclude filters if not empty
	mustNotClause := strings.Join(excludeFilterStrings, ",")
	if len(mustNotClause) > 0 {
		mustNotClause = fmt.Sprintf(`"must_not": [%s]`, mustNotClause)
		boolClause = append(boolClause, mustNotClause)
	}

	query := fmt.Sprintf(`{
		"query": {
			"bool": {%s}
		},
		"sort": [{"tickNumber":{"order":"desc"}},{"logId":{"order":"asc"}}],
		"from": %d,
		"size": %d,
		"track_total_hits": %d
	}`, strings.Join(boolClause, ","), from, size, maxTrackTotalHits)
	// log.Printf("[DEBUG] %s", query)
	return query, nil
}
