package elastic

import (
	"encoding/json"
	"testing"

	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_createEventsQuery_noFilters(t *testing.T) {
	query, err := createEventsQuery(entities.Filters{}, 0, 10)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err, "query should be valid JSON")

	// Verify query structure
	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)
	assert.Empty(t, boolQuery, "no filters should be present")

	assert.Equal(t, float64(0), parsed["from"])
	assert.Equal(t, float64(10), parsed["size"])
	assert.Equal(t, float64(maxTrackTotalHits), parsed["track_total_hits"])

	// Verify sort
	sort := parsed["sort"].([]any)
	assert.Len(t, sort, 2)

	tickNumberSort := sort[0].(map[string]interface{})
	require.Contains(t, tickNumberSort, "tickNumber")
	tickNumberOrder := tickNumberSort["tickNumber"].(map[string]interface{})["order"]
	require.Contains(t, tickNumberOrder, "desc")

	logIdSort := sort[1].(map[string]interface{})
	require.Contains(t, logIdSort, "logId")
	logIdOrder := logIdSort["logId"].(map[string]interface{})["order"]
	require.Contains(t, logIdOrder, "asc")
}

func Test_createEventsQuery_withTransactionHash(t *testing.T) {
	filters := map[string][]string{
		"transactionHash": {"abc123"},
	}
	f := entities.Filters{
		Include: filters,
	}
	query, err := createEventsQuery(f, 0, 10)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)
	filterArr := boolQuery["filter"].([]any)
	require.Len(t, filterArr, 1)

	termFilter := filterArr[0].(map[string]any)["term"].(map[string]any)
	assert.Equal(t, "abc123", termFilter["transactionHash"])
}

func Test_createEventsQuery_withTickNumber(t *testing.T) {
	filters := map[string][]string{
		"tickNumber": {"42"},
	}
	f := entities.Filters{
		Include: filters,
	}
	query, err := createEventsQuery(f, 0, 10)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)
	filterArr := boolQuery["filter"].([]any)
	require.Len(t, filterArr, 1)

	termFilter := filterArr[0].(map[string]any)["term"].(map[string]any)
	assert.Equal(t, "42", termFilter["tickNumber"])
}

func Test_createEventsQuery_withEventType(t *testing.T) {
	filters := map[string][]string{
		"logType": {"1"},
	}
	f := entities.Filters{
		Include: filters,
	}
	query, err := createEventsQuery(f, 0, 10)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)
	filterArr := boolQuery["filter"].([]any)
	require.Len(t, filterArr, 1)

	// eventType should map to ES field "type"
	termFilter := filterArr[0].(map[string]any)["term"].(map[string]any)
	assert.Equal(t, "1", termFilter["type"])
}

func Test_createEventsQuery_withMultipleFilters(t *testing.T) {
	filters := map[string][]string{
		"transactionHash": {"abc123"},
		"tickNumber":      {"42"},
		"logType":         {"2"},
	}
	f := entities.Filters{
		Include: filters,
	}
	query, err := createEventsQuery(f, 0, 10)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)
	filterArr := boolQuery["filter"].([]any)
	assert.Len(t, filterArr, 3)
}

func Test_createEventsQuery_withPagination(t *testing.T) {
	query, err := createEventsQuery(entities.Filters{}, 20, 50)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	assert.Equal(t, float64(20), parsed["from"])
	assert.Equal(t, float64(50), parsed["size"])
}

func Test_createEventsQuery_withExcludeFilter(t *testing.T) {
	f := entities.Filters{
		Include: map[string][]string{
			"source": {"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Exclude: map[string][]string{
			"destination": {"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
		},
	}
	query, err := createEventsQuery(f, 0, 10)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)

	// Verify include filter
	filterArr := boolQuery["filter"].([]any)
	require.Len(t, filterArr, 1)
	termFilter := filterArr[0].(map[string]any)["term"].(map[string]any)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", termFilter["source"])

	// Verify exclude filter
	mustNotArr := boolQuery["must_not"].([]any)
	require.Len(t, mustNotArr, 1)
	mustNotTerm := mustNotArr[0].(map[string]any)["term"].(map[string]any)
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", mustNotTerm["destination"])
}

func Test_createEventsQuery_withOnlyExcludeFilter(t *testing.T) {
	filters := map[string][]string{
		"destination": {"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"},
	}
	f := entities.Filters{
		Exclude: filters,
	}
	query, err := createEventsQuery(f, 0, 10)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)

	// Verify no include filters
	_, hasFilter := boolQuery["filter"]
	assert.False(t, hasFilter, "should not have filter clause")

	// Verify exclude filter
	mustNotArr := boolQuery["must_not"].([]any)
	require.Len(t, mustNotArr, 1)
	mustNotTerm := mustNotArr[0].(map[string]any)["term"].(map[string]any)
	assert.Equal(t, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC", mustNotTerm["destination"])
}

func Test_createEventsQuery_withRangeFilter(t *testing.T) {
	ranges := map[string][]entities.Range{
		"amount": {
			{Operation: "gte", Value: "100"},
			{Operation: "lte", Value: "1000"},
		},
		"tickNumber": {
			{Operation: "gt", Value: "123"},
		},
	}
	query, err := createEventsQuery(entities.Filters{Ranges: ranges}, 0, 10)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)
	filterArr := boolQuery["filter"].([]any)
	require.Len(t, filterArr, 2)

	rangeFilter := filterArr[0].(map[string]any)["range"].(map[string]any)
	amountRange := rangeFilter["amount"].(map[string]any)
	assert.Equal(t, "100", amountRange["gte"])
	assert.Equal(t, "1000", amountRange["lte"])

	rangeFilter = filterArr[1].(map[string]any)["range"].(map[string]any)
	tickNumberRange := rangeFilter["tickNumber"].(map[string]any)
	assert.Equal(t, "123", tickNumberRange["gt"])
}

func Test_createEventsQuery_withTwoShouldFilters(t *testing.T) {
	shouldFilters := []entities.ShouldFilter{
		{
			Terms: map[string][]string{
				"source":      {"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
				"destination": {"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
			},
		},
		{
			Terms: map[string][]string{
				"destination": {"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
			},
		},
	}
	f := entities.Filters{
		Should: shouldFilters,
	}
	query, err := createEventsQuery(f, 0, 10)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)
	filterArr := boolQuery["filter"].([]any)
	require.Len(t, filterArr, 2)

	// Verify first should filter
	firstShouldBool := filterArr[0].(map[string]any)["bool"].(map[string]any)
	firstShouldArr := firstShouldBool["should"].([]any)
	require.Len(t, firstShouldArr, 2)
	assert.Equal(t, float64(1), firstShouldBool["minimum_should_match"])
	destinationTerm := firstShouldArr[0].(map[string]any)["term"].(map[string]any) // ordered alphabetically
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", destinationTerm["destination"])
	sourceTerm := firstShouldArr[1].(map[string]any)["term"].(map[string]any)
	assert.Equal(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", sourceTerm["source"])

	// Verify second should filter
	secondShouldBool := filterArr[1].(map[string]any)["bool"].(map[string]any)
	secondShouldArr := secondShouldBool["should"].([]any)
	require.Len(t, secondShouldArr, 1)
	assert.Equal(t, float64(1), secondShouldBool["minimum_should_match"])
	secondTermFilter := secondShouldArr[0].(map[string]any)["term"].(map[string]any)
	assert.Equal(t, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", secondTermFilter["destination"])
}
