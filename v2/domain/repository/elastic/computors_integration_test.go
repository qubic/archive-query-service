package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/google/go-cmp/cmp/cmpopts"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcelastic "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testComputorList1 = computorsList{
	Epoch:      105,
	TickNumber: 13461049,
	Identities: []string{
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	},
	Signature: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
}

type computorsSuite struct {
	suite.Suite
	repo      *Repository
	ctx       context.Context
	container testcontainers.Container
}

func TestComputorsListRepository(t *testing.T) {
	suite.Run(t, new(computorsSuite))
}

func (s *computorsSuite) TearDownSuite() {
	s.container.Terminate(s.ctx) //nolint: errcheck
}

func (s *computorsSuite) SetupSuite() {
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

	computorsListMapping := `
	{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0,
			"index": {
				"sort.field": "tickNumber",
				"sort.order": "desc"
			}
		},
		"mappings": {
            "dynamic": "strict",
    		  "properties": {
    		    "epoch": {
    		      "type": "unsigned_long"
    		    },
    		    "tickNumber": {
    		      "type": "unsigned_long"
    		    },
    		    "identities": {
    		      "type": "keyword",
    		      "ignore_above": 60
    		    },
    		    "signature": {
    		      "type": "keyword"
    		    }
			}
        }
	}`

	res, err := esClient.Indices.Create("qubic-computors", esClient.Indices.Create.WithBody(strings.NewReader(computorsListMapping)))
	require.NoError(s.T(), err, "creating computors list index")
	defer res.Body.Close()
	assert.NoError(s.T(), err)
	require.False(s.T(), res.IsError(), "creating computors list index should be successful")

	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(testComputorList1)
	require.NoError(s.T(), err, "encoding computors list")
	res, err = esClient.Index(
		"qubic-computors",
		&buf,
		esClient.Index.WithDocumentID("105"),
		esClient.Index.WithRefresh("true"),
	)

	require.NoError(s.T(), err, "indexing computors list")
	defer res.Body.Close()
	require.Falsef(s.T(), res.IsError(), "indexing computors list should be successful, got err: %s", res.String())

	s.repo = NewRepository("transactions", "tick-data", "qubic-computors", esClient)
}

func (s *computorsSuite) Test_GetEpochComputorsList() {

	cl, err := s.repo.GetComputorsListsForEpoch(s.ctx, testComputorList1.Epoch)
	require.NoError(s.T(), err, "getting computors list")
	expected := computorsListToAPIObject(testComputorList1)

	// Wrap in slice, as that is what the GetComputorsListsForEpoch method returns
	assert.Equal(s.T(), []*api.ComputorList{expected}, cl, cmpopts.IgnoreUnexported(api.ComputorList{}))

}
