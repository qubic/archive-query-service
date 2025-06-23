package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcelastic "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
	"strings"
	"testing"
	"time"
)

var testTickData1 = tickData{
	ComputorIndex: 10,
	Epoch:         100,
	TickNumber:    12312312,
	Timestamp:     3141592653897,
	VarStruct:     "a",
	Timelock:      "a",
	TransactionHashes: []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	},
	ContractFees: nil,
	Signature:    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
}

type tickDataSuite struct {
	suite.Suite
	repo      *Repository
	ctx       context.Context
	container testcontainers.Container
}

func TestTickDataRepository(t *testing.T) {
	suite.Run(t, new(tickDataSuite))
}

func (t *tickDataSuite) TearDownSuite() {
	t.container.Terminate(t.ctx)
}

func (t *tickDataSuite) SetupSuite() {
	t.ctx = context.Background()

	container, err := tcelastic.Run(
		t.ctx,
		"docker.elastic.co/elasticsearch/elasticsearch:8.10.2",
		tcelastic.WithPassword("password"),
		testcontainers.WithWaitStrategy(wait.ForLog("\"message\":\"started").WithStartupTimeout(1*time.Minute)),
	)
	require.NoError(t.T(), err)
	t.container = container

	elsCfg := elasticsearch.Config{
		Addresses: []string{container.Settings.Address},
		Username:  "elastic",
		Password:  "password",
		CACert:    container.Settings.CACert,
	}

	esClient, err := elasticsearch.NewClient(elsCfg)
	require.NoError(t.T(), err, "creating elasticsearch client")

	resp, err := esClient.Info()
	require.NoError(t.T(), err, "getting elastic info")
	defer resp.Body.Close()

	var esResp elasticsearchTestResponse
	err = json.NewDecoder(resp.Body).Decode(&esResp)
	require.NoError(t.T(), err, "decoding elasticsearch response")
	require.Equal(t.T(), "You Know, for Search", esResp.Tagline)

	tickDataMapping := `
	{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		},
		"mappings": {
            "dynamic": "strict",
            "properties": {
                "computorIndex": {
                    "type": "unsigned_long"
                },
                "contractFees": {
                    "type": "unsigned_long",
                    "index": false
                },
                "epoch": {
                    "type": "unsigned_long"
                },
                "signature": {
                    "type": "keyword",
                    "index": false,
                    "doc_values": false
                },
                "tickNumber": {
                    "type": "unsigned_long"
                },
                "timeLock": {
                    "type": "keyword",
                    "index": false,
                    "doc_values": false
                },
                "timestamp": {
                    "type": "unsigned_long"
                },
                "transactionHashes": {
                    "type": "keyword"
                },
                "varStruct": {
                    "type": "keyword",
                    "index": false,
                    "doc_values": false
                }
            }
        }
	}`

	res, err := esClient.Indices.Create("tick-data",
		esClient.Indices.Create.WithBody(strings.NewReader(tickDataMapping)),
	)
	require.NoError(t.T(), err, "creating tick data index")
	defer res.Body.Close()
	require.False(t.T(), res.IsError(), "creating tick data index should be successful")

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(testTickData1)
	require.NoError(t.T(), err, "encoding test tick data")
	res, err = esClient.Index(
		"tick-data",
		&buf,
		esClient.Index.WithDocumentID("12312312"),
		esClient.Index.WithRefresh("true"),
	)

	require.NoError(t.T(), err, "indexing test tick data")
	defer res.Body.Close()
	require.Falsef(t.T(), res.IsError(), "indexing test tick data should be successful, got err: %s", res.String())

	t.repo = NewRepository("transactions", "tick-data", esClient)
}

func (t *tickDataSuite) Test_GetTickData() {

	td, err := t.repo.GetTickData(t.ctx, testTickData1.TickNumber)
	require.NoError(t.T(), err, "getting tick data")
	expected := tickDataToApiTickData(testTickData1)
	diff := cmp.Diff(expected, td, cmpopts.IgnoreUnexported(api.TickData{}))
	require.Empty(t.T(), diff, "tick data received should match the one inserted, diff: %s", diff)
}
