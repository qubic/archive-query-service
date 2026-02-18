package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcelastic "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testEvent1 = event{
	Epoch:                 100,
	TickNumber:            15000,
	Timestamp:             1700000001,
	EmittingContractIndex: 1,
	TransactionHash:       "txhash1",
	LogID:                 1,
	LogDigest:             "digest1",
	Type:                  0,
	Category:              0,
	Source:                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
	Destination:           "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
	Amount:                1000,
}

var testEvent2 = event{
	Epoch:                 100,
	TickNumber:            15001,
	Timestamp:             1700000002,
	EmittingContractIndex: 1,
	TransactionHash:       "txhash2",
	LogID:                 2,
	LogDigest:             "digest2",
	Type:                  1,
	Category:              1,
	Source:                "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
	Destination:           "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
	Amount:                2000,
}

var testEvent3 = event{
	Epoch:                 100,
	TickNumber:            15002,
	Timestamp:             1700000003,
	EmittingContractIndex: 2,
	TransactionHash:       "txhash1",
	LogID:                 3,
	LogDigest:             "digest3",
	Type:                  2,
	Category:              0,
	AssetName:             "QX",
	AssetIssuer:           "ISSUER",
	NumberOfShares:        500,
}

var testEvent4 = event{
	Epoch:                 101,
	TickNumber:            16000,
	Timestamp:             1700000004,
	EmittingContractIndex: 3,
	TransactionHash:       "txhash3",
	LogID:                 4,
	LogDigest:             "digest4",
	Type:                  3,
	Category:              0,
}

var testEvent5 = event{
	Epoch:           101,
	TickNumber:      16001,
	Timestamp:       1700000005,
	TransactionHash: "txhash4",
	LogID:           5,
	Type:            8,
}

var testEvent6 = event{
	Epoch:           101,
	TickNumber:      16002,
	Timestamp:       1700000006,
	TransactionHash: "txhash5",
	LogID:           6,
	Type:            13,
}

type eventsSuite struct {
	suite.Suite
	repo      *EventsRepository
	ctx       context.Context
	container testcontainers.Container
}

func TestEventsRepository(t *testing.T) {
	suite.Run(t, new(eventsSuite))
}

func (s *eventsSuite) TearDownSuite() {
	s.container.Terminate(s.ctx) //nolint: errcheck
}

func (s *eventsSuite) SetupSuite() {
	s.ctx = context.Background()

	container, err := tcelastic.Run(
		s.ctx,
		"docker.elastic.co/elasticsearch/elasticsearch:8.10.2",
		tcelastic.WithPassword("password"),
		testcontainers.WithWaitStrategy(wait.ForLog("\"message\":\"started").WithStartupTimeout(1*time.Minute)),
	)
	require.NoError(s.T(), err)
	s.container = container

	elsCfg := elasticsearch.Config{
		Addresses: []string{container.Settings.Address},
		Username:  "elastic",
		Password:  "password",
		CACert:    container.Settings.CACert,
	}

	esClient, err := elasticsearch.NewClient(elsCfg)
	require.NoError(s.T(), err, "creating elasticsearch client")

	resp, err := esClient.Info()
	require.NoError(s.T(), err, "getting elastic info")
	defer resp.Body.Close()

	var esResp elasticsearchTestResponse
	err = json.NewDecoder(resp.Body).Decode(&esResp)
	require.NoError(s.T(), err, "decoding elasticsearch response")
	require.Equal(s.T(), "You Know, for Search", esResp.Tagline)

	eventsMapping := `
	{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		},
		"mappings": {
			"properties": {
				"epoch": { "type": "unsigned_long" },
				"tickNumber": { "type": "unsigned_long" },
				"timestamp": { "type": "unsigned_long" },
				"emittingContractIndex": { "type": "unsigned_long" },
				"transactionHash": { "type": "keyword" },
				"logId": { "type": "unsigned_long" },
				"logDigest": { "type": "keyword" },
				"type": { "type": "unsigned_long" },
				"category": { "type": "unsigned_long" },
				"source": { "type": "keyword" },
				"destination": { "type": "keyword" },
				"amount": { "type": "unsigned_long" },
				"assetName": { "type": "keyword" },
				"assetIssuer": { "type": "keyword" },
				"numberOfShares": { "type": "unsigned_long" },
				"managingContractIndex": { "type": "unsigned_long" },
				"unitOfMeasurement": { "type": "keyword" },
				"numberOfDecimalPlaces": { "type": "unsigned_long" },
				"deductedAmount": { "type": "unsigned_long" },
				"remainingAmount": { "type": "long" },
				"contractIndex": { "type": "unsigned_long" },
				"contractIndexBurnedFor": { "type": "unsigned_long" }
			}
		}
	}`

	res, err := esClient.Indices.Create(
		"qubic-event-logs",
		esClient.Indices.Create.WithBody(strings.NewReader(eventsMapping)),
	)
	require.NoError(s.T(), err, "creating events index")
	defer res.Body.Close()
	require.False(s.T(), res.IsError(), "creating events index should be successful")

	s.indexEvent(esClient, testEvent1, "1")
	s.indexEvent(esClient, testEvent2, "2")
	s.indexEvent(esClient, testEvent3, "3")
	s.indexEvent(esClient, testEvent4, "4")
	s.indexEvent(esClient, testEvent5, "5")
	s.indexEvent(esClient, testEvent6, "6")

	s.repo = NewEventsRepository("qubic-event-logs", esClient)
}

func (s *eventsSuite) indexEvent(esClient *elasticsearch.Client, ev event, docID string) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(ev)
	require.NoError(s.T(), err, "encoding test event")
	res, err := esClient.Index(
		"qubic-event-logs",
		&buf,
		esClient.Index.WithDocumentID(docID),
		esClient.Index.WithRefresh("true"),
	)
	require.NoError(s.T(), err, "indexing test event")
	defer res.Body.Close()
	require.Falsef(s.T(), res.IsError(), "indexing test event should be successful, got err: %s", res.String())
}

func (s *eventsSuite) Test_GetEvents_NoFilters() {
	events, hits, err := s.repo.GetEvents(s.ctx, nil, 0, 10)
	require.NoError(s.T(), err, "getting events without filters")
	assert.Len(s.T(), events, 6)
	assert.Equal(s.T(), 6, hits.Total)
}

func (s *eventsSuite) Test_GetEvents_FilterByTransactionHash() {
	filters := map[string][]string{"transactionHash": {"txhash1"}}
	events, hits, err := s.repo.GetEvents(s.ctx, filters, 0, 10)
	require.NoError(s.T(), err, "getting events by transaction hash")
	assert.Len(s.T(), events, 2)
	assert.Equal(s.T(), 2, hits.Total)

	for _, ev := range events {
		assert.Equal(s.T(), "txhash1", ev.TransactionHash)
	}
}

func (s *eventsSuite) Test_GetEvents_FilterByTickNumber() {
	filters := map[string][]string{"tickNumber": {"15001"}}
	events, hits, err := s.repo.GetEvents(s.ctx, filters, 0, 10)
	require.NoError(s.T(), err, "getting events by tick number")
	require.Len(s.T(), events, 1)
	assert.Equal(s.T(), 1, hits.Total)

	expected := eventToAPIEvent(testEvent2)
	diff := cmp.Diff(expected, events[0], protocmp.Transform())
	assert.Empty(s.T(), diff, "event should match. diff: %s", diff)
}

func (s *eventsSuite) Test_GetEvents_FilterByEventType() {
	filters := map[string][]string{"eventType": {"8"}}
	events, hits, err := s.repo.GetEvents(s.ctx, filters, 0, 10)
	require.NoError(s.T(), err, "getting events by event type")
	require.Len(s.T(), events, 1)
	assert.Equal(s.T(), 1, hits.Total)
	assert.Equal(s.T(), uint32(8), events[0].EventType)
}

func (s *eventsSuite) Test_GetEvents_CombinedFilters() {
	filters := map[string][]string{
		"transactionHash": {"txhash1"},
		"eventType":       {"0"},
	}
	events, hits, err := s.repo.GetEvents(s.ctx, filters, 0, 10)
	require.NoError(s.T(), err, "getting events with combined filters")
	require.Len(s.T(), events, 1)
	assert.Equal(s.T(), 1, hits.Total)
	assert.Equal(s.T(), "txhash1", events[0].TransactionHash)
	assert.Equal(s.T(), uint32(0), events[0].EventType)
}

func (s *eventsSuite) Test_GetEvents_Pagination() {
	// Get first page of 2
	events1, hits1, err := s.repo.GetEvents(s.ctx, nil, 0, 2)
	require.NoError(s.T(), err, "getting first page")
	assert.Len(s.T(), events1, 2)
	assert.Equal(s.T(), 6, hits1.Total)

	// Get second page of 2
	events2, hits2, err := s.repo.GetEvents(s.ctx, nil, 2, 2)
	require.NoError(s.T(), err, "getting second page")
	assert.Len(s.T(), events2, 2)
	assert.Equal(s.T(), 6, hits2.Total)

	// Pages should have different events
	assert.NotEqual(s.T(), events1[0].LogId, events2[0].LogId)
}

func (s *eventsSuite) Test_GetEvents_NoResults() {
	filters := map[string][]string{"transactionHash": {"nonexistent"}}
	events, hits, err := s.repo.GetEvents(s.ctx, filters, 0, 10)
	require.NoError(s.T(), err, "getting events with no results")
	assert.Len(s.T(), events, 0)
	assert.Equal(s.T(), 0, hits.Total)
}
