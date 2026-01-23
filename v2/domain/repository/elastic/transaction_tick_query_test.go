package elastic

import (
	"encoding/json"
	"testing"

	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_createTickTransactionsQuery_noFilters(t *testing.T) {
	query, err := createTickTransactionsQuery(12345, nil, nil)
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(query.Bytes(), &parsed)
	require.NoError(t, err)

	// Verify track_total_hits
	assert.Equal(t, true, parsed["track_total_hits"])

	// Verify size
	assert.Equal(t, float64(1024), parsed["size"])

	// Verify query structure
	queryBlock, ok := parsed["query"].(map[string]interface{})
	require.True(t, ok)
	boolBlock, ok := queryBlock["bool"].(map[string]interface{})
	require.True(t, ok)
	filterBlock, ok := boolBlock["filter"].([]interface{})
	require.True(t, ok)

	// Should have exactly one filter (tickNumber)
	assert.Len(t, filterBlock, 1)
}

func Test_createTickTransactionsQuery_withFilters(t *testing.T) {
	filters := map[string][]string{
		"source":    {"SOMESOURCEIDENTITY123456789012345678901234567890123456"},
		"amount":    {"100"},
		"inputType": {"1"},
	}
	query, err := createTickTransactionsQuery(42, filters, nil)
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(query.Bytes(), &parsed)
	require.NoError(t, err)

	// Verify query structure
	queryBlock := parsed["query"].(map[string]interface{})
	boolBlock := queryBlock["bool"].(map[string]interface{})
	filterBlock := boolBlock["filter"].([]interface{})

	// Should have tickNumber + 3 filters = 4 total
	assert.Len(t, filterBlock, 4)
}

func Test_createTickTransactionsQuery_withRanges(t *testing.T) {
	ranges := map[string][]*entities.Range{
		"amount": {
			{Operation: "gte", Value: "1000"},
			{Operation: "lte", Value: "10000"},
		},
		"inputType": {
			{Operation: "gt", Value: "0"},
		},
	}
	query, err := createTickTransactionsQuery(42, nil, ranges)
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(query.Bytes(), &parsed)
	require.NoError(t, err)

	// Verify query structure
	queryBlock := parsed["query"].(map[string]interface{})
	boolBlock := queryBlock["bool"].(map[string]interface{})
	filterBlock := boolBlock["filter"].([]interface{})

	// Should have tickNumber + 2 ranges = 3 total
	assert.Len(t, filterBlock, 3)
}

func Test_createTickTransactionsQuery_withFiltersAndRanges(t *testing.T) {
	filters := map[string][]string{
		"destination": {"SOMEDESTIDENTITY123456789012345678901234567890123456"},
	}
	ranges := map[string][]*entities.Range{
		"amount": {
			{Operation: "gte", Value: "100"},
		},
	}
	query, err := createTickTransactionsQuery(999, filters, ranges)
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(query.Bytes(), &parsed)
	require.NoError(t, err)

	// Verify query structure
	queryBlock := parsed["query"].(map[string]interface{})
	boolBlock := queryBlock["bool"].(map[string]interface{})
	filterBlock := boolBlock["filter"].([]interface{})

	// Should have tickNumber + 1 filter + 1 range = 3 total
	assert.Len(t, filterBlock, 3)
}
