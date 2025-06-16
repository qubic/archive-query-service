package elastic

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcelastic "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
	"strings"
	"testing"
	"time"
)

var testTx1 = transaction{
	Hash:        "zvqvtjzvgwgpegmalkkjedhbdrnckqcfthpzfqzxbcljttljzidmvaxalwvk",
	Source:      "ENYTRGQOXEUCDFYZUSJTKTKJIZJABAHZQQANAQCPDBKJRDAZQIFMGIRDWGPO",
	Destination: "KDPFLKJDPLRPZGLWNGPYBPSOXONATJZEIQZQPMWLTDWTGAFOKGNTZMFAMSAA",
	Amount:      10,
	TickNumber:  15,
	InputType:   1,
	InputSize:   0,
	InputData:   base64.StdEncoding.EncodeToString([]byte("test input data")),
	Signature:   base64.StdEncoding.EncodeToString([]byte("test signature")),
	Timestamp:   uint64(time.Now().Unix()),
	MoneyFlew:   true,
}

type elasticsearchTestResponse struct {
	Name        string `json:"name"`
	ClusterName string `json:"cluster_name"`
	ClusterUUID string `json:"cluster_uuid"`
	Version     struct {
		Number string `json:"number"`
	} `json:"version"`
	Tagline string `json:"tagline"`
}

type transactionsSuite struct {
	suite.Suite
	repo      *Repository
	ctx       context.Context
	container testcontainers.Container
}

func TestTransactionsRepository(t *testing.T) {
	suite.Run(t, new(transactionsSuite))
}

func (t *transactionsSuite) TearDownSuite() {
	t.container.Terminate(t.ctx)
}

func (t *transactionsSuite) SetupSuite() {
	t.ctx = context.Background()

	container, err := tcelastic.Run(
		t.ctx,
		"docker.elastic.co/elasticsearch/elasticsearch:8.10.2",
		tcelastic.WithPassword("password"),
		testcontainers.WithWaitStrategy(wait.ForLog("\"message\":\"started").WithStartupTimeout(15*time.Second)),
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

	transactionsMapping := `
	{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		},
		"mappings": {
		  "dynamic": "strict",
		  "properties": {
			"amount": {
			  "type": "unsigned_long"
			},
			"destination": {
			  "type": "keyword",
			  "ignore_above": 60
			},
			"hash": {
			  "type": "keyword",
			  "ignore_above": 60
			},
			"inputData": {
			  "type": "keyword",
			  "index": false,
			  "doc_values": false
			},
			"inputSize": {
			  "type": "unsigned_long",
			  "index": false
			},
			"inputType": {
			  "type": "unsigned_long"
			},
			"moneyFlew": {
			  "type": "boolean"
			},
			"signature": {
			  "type": "keyword",
			  "index": false,
			  "doc_values": false
			},
			"source": {
			  "type": "keyword",
			  "ignore_above": 60
			},
			"tickNumber": {
			  "type": "unsigned_long"
			},
			"timestamp": {
			  "type": "unsigned_long"
			}
		  }
		}
	}`

	res, err := esClient.Indices.Create(
		"transactions",
		esClient.Indices.Create.WithBody(strings.NewReader(transactionsMapping)),
	)
	require.NoError(t.T(), err, "creating transactions index")
	defer res.Body.Close()
	require.False(t.T(), res.IsError(), "creating transactions index should be successful")

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(testTx1)
	require.NoError(t.T(), err, "encoding test transaction")
	res, err = esClient.Index(
		"transactions",
		&buf,
		esClient.Index.WithDocumentID("zvqvtjzvgwgpegmalkkjedhbdrnckqcfthpzfqzxbcljttljzidmvaxalwvk"),
		esClient.Index.WithRefresh("true"),
	)
	require.NoError(t.T(), err, "indexing test transaction")
	defer res.Body.Close()
	require.Falsef(t.T(), res.IsError(), "indexing test transaction should be successful, got err: %s", res.String())

	t.repo = NewRepository("transactions", "tick-data", esClient)
}

func (t *transactionsSuite) Test_GetTransactionByHash() {
	tx, err := t.repo.GetTransactionByHash(t.ctx, testTx1.Hash)
	require.NoError(t.T(), err, "getting transaction by hash")
	expected := transactionToApiTransaction(testTx1)
	diff := cmp.Diff(expected, tx, cmpopts.IgnoreUnexported(api.Transaction{}))
	require.Empty(t.T(), diff, "transaction received should match the one inserted, diff: %s", diff)
}
