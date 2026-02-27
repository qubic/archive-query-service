package elastic

import (
	"log"
	"testing"

	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/require"
)

var testIdentity = "some-identity"

func Test_createIdentitiesQuery_returnQuery(t *testing.T) {
	expectedQuery := `{ 
      "query": {
		"bool": {
		  "should": [
			{ "term":{"source":"some-identity"} },
			{ "term":{"destination":"some-identity"} }
		  ],
		  "minimum_should_match": 1,
		  "filter": [{"range":{"tickNumber":{"lte":"12345"}}}]
		}
	  },
	  "sort": [ {"tickNumber":{"order":"desc"}} ],
	  "from": 0,
	  "size": 10,
	  "track_total_hits": 10000
	}`

	query, err := createIdentitiesQuery(testIdentity, nil, nil, 0, 10, 12345)
	require.NoError(t, err)
	require.NotEmpty(t, query)

	require.JSONEq(t, expectedQuery, query)
}

func Test_createIdentitiesQuery_givenFilters_returnQueryWithFilters(t *testing.T) {
	expectedQuery := `{ 
      "query": {
		"bool": {
		  "should": [
			{ "term":{"source":"some-identity"} },
			{ "term":{"destination":"some-identity"} }
		  ],
		  "minimum_should_match": 1,
		  "filter": [
            {"range":{"tickNumber":{"lte":"1000000"}}},
			{"term":{"another-value":"foo"}},
			{"term":{"some-value":"42"}}
          ]
		}
	  },
	  "sort": [ {"tickNumber":{"order":"desc"}} ],
	  "from": 0,
	  "size": 5,
	  "track_total_hits": 10000
	}`

	filters := map[string][]string{"some-value": {"42"}, "another-value": {"foo"}}
	query, err := createIdentitiesQuery(testIdentity, filters, nil, 0, 5, 1000000)
	require.NoError(t, err)
	require.NotEmpty(t, query)

	require.JSONEq(t, expectedQuery, query)
}

func Test_createIdentitiesQuery_givenExcludeFilters_returnQueryWithExcludeFilters(t *testing.T) {
	expectedQuery := `{ 
      "query": {
		"bool": {
		  "should": [
			{ "term":{"source":"some-identity"} },
			{ "term":{"destination":"some-identity"} }
		  ],
		  "minimum_should_match": 1,
		  "filter": [ 
			{"range":{"tickNumber":{"lte":"1000000"}}}
		  ],
		  "must_not": [
			{"term":{"another-value":"foo"}},
			{"term":{"some-value":"42"}}
		  ]
		}
	  },
	  "sort": [ {"tickNumber":{"order":"desc"}} ],
	  "from": 0,
	  "size": 5,
	  "track_total_hits": 10000
	}`

	filters := map[string][]string{"some-value-exclude": {"42"}, "another-value-exclude": {"foo"}}
	query, err := createIdentitiesQuery(testIdentity, filters, nil, 0, 5, 1000000)
	require.NoError(t, err)
	require.NotEmpty(t, query)

	require.JSONEq(t, expectedQuery, query)
}

func Test_createIdentitiesQuery_givenRanges_returnQueryWithFilters(t *testing.T) {
	expectedQuery := `{ 
      "query": {
		"bool": {
		  "should": [
			{ "term":{"source":"some-identity"} },
			{ "term":{"destination":"some-identity"} }
		  ],
		  "minimum_should_match": 1,
		  "filter": [
            {"range":{"tickNumber":{"lte":"1000000"}}},
			{"range":{"another-value":{ "lte": "43", "gte": "12"  }}},
			{"range":{"some-value":{ "lt": "42" }}},
			{"range":{"third-value":{ "gt": "44"}}}
          ]
		}
	  },
	  "sort": [ {"tickNumber":{"order":"desc"}} ],
	  "from": 0,
	  "size": 5,
	  "track_total_hits": 10000
	}`

	range1 := []*entities.Range{{Operation: "lt", Value: "42"}}
	range2 := []*entities.Range{{Operation: "gte", Value: "12"}, {Operation: "lte", Value: "43"}}
	range3 := []*entities.Range{{Operation: "gt", Value: "44"}}
	ranges := map[string][]*entities.Range{"some-value": range1, "another-value": range2, "third-value": range3}
	query, err := createIdentitiesQuery(testIdentity, nil, ranges, 0, 5, 1000000)
	require.NoError(t, err)
	require.NotEmpty(t, query)

	log.Println(query)
	require.JSONEq(t, expectedQuery, query)
}

func Test_createIdentitiesQuery_givenRangesAndFilters_returnQueryWithAllFilters(t *testing.T) {
	expectedQuery := `{ 
      "query": {
		"bool": {
		  "should": [
			{ "term":{"source":"some-identity"} },
			{ "term":{"destination":"some-identity"} }
		  ],
		  "minimum_should_match": 1,
		  "filter": [
            {"range": {"tickNumber": {"lte":"1000000"} } },
			{"term": {"another-value": "foo"}},
		    {"term": {"some-value": "42"} },
			{"range": {"range-value": { "gt": "0", "lte": "42" } } }
          ],
		  "must_not": [
			{"terms": {"other-value": ["exclude-me", "exclude-me-too"] } }
		  ]
		}
	  },
	  "sort": [ {"tickNumber": {"order":"desc"} } ],
	  "from": 200,
	  "size": 100,
	  "track_total_hits": 10000
	}`

	range1 := []*entities.Range{{Operation: "lte", Value: "42"}, {Operation: "gt", Value: "0"}}
	ranges := map[string][]*entities.Range{"range-value": range1}
	filters := map[string][]string{"some-value": {"42"}, "another-value": {"foo"}, "other-value-exclude": {"exclude-me", "exclude-me-too"}}
	query, err := createIdentitiesQuery(testIdentity, filters, ranges, 200, 100, 1000000)
	require.NoError(t, err)
	require.NotEmpty(t, query)

	log.Println(query)
	require.JSONEq(t, expectedQuery, query)
}
