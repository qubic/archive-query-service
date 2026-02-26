package elastic

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_createEventsQuery_noFilters(t *testing.T) {
	query := createEventsQuery(nil, 0, 10)

	var parsed map[string]any
	err := json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err, "query should be valid JSON")

	// Verify query structure
	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)
	filters := boolQuery["filter"].([]any)
	assert.Empty(t, filters, "no filters should be present")

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
	query := createEventsQuery(filters, 0, 10)

	var parsed map[string]any
	err := json.Unmarshal([]byte(query), &parsed)
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
	query := createEventsQuery(filters, 0, 10)

	var parsed map[string]any
	err := json.Unmarshal([]byte(query), &parsed)
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
		"eventType": {"1"},
	}
	query := createEventsQuery(filters, 0, 10)

	var parsed map[string]any
	err := json.Unmarshal([]byte(query), &parsed)
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
		"eventType":       {"2"},
	}
	query := createEventsQuery(filters, 0, 10)

	var parsed map[string]any
	err := json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	q := parsed["query"].(map[string]any)
	boolQuery := q["bool"].(map[string]any)
	filterArr := boolQuery["filter"].([]any)
	assert.Len(t, filterArr, 3)
}

func Test_createEventsQuery_withPagination(t *testing.T) {
	query := createEventsQuery(nil, 20, 50)

	var parsed map[string]any
	err := json.Unmarshal([]byte(query), &parsed)
	require.NoError(t, err)

	assert.Equal(t, float64(20), parsed["from"])
	assert.Equal(t, float64(50), parsed["size"])
}
