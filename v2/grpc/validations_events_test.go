package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateEventsFilters_ValidFilters(t *testing.T) {
	filters := map[string]string{
		"transactionHash": "abc123",
		"tickNumber":      "42",
		"eventType":       "1",
	}
	result, err := createEventsFilters(filters)
	require.NoError(t, err)
	assert.Equal(t, map[string][]string{
		"transactionHash": {"abc123"},
		"tickNumber":      {"42"},
		"eventType":       {"1"},
	}, result)
}

func TestCreateEventsFilters_EmptyValue(t *testing.T) {
	filters := map[string]string{
		"transactionHash": "",
	}
	_, err := createEventsFilters(filters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty value")
}

func TestValidateEventsFilters_ValidTransactionHash(t *testing.T) {
	filters := map[string][]string{"transactionHash": {"abc123"}}
	err := validateEventsFilters(filters)
	require.NoError(t, err)
}

func TestValidateEventsFilters_ValidTickNumber(t *testing.T) {
	filters := map[string][]string{"tickNumber": {"42"}}
	err := validateEventsFilters(filters)
	require.NoError(t, err)
}

func TestValidateEventsFilters_ValidEventType(t *testing.T) {
	for _, et := range []string{"0", "1", "2", "3", "8", "13"} {
		t.Run("eventType_"+et, func(t *testing.T) {
			filters := map[string][]string{"eventType": {et}}
			err := validateEventsFilters(filters)
			require.NoError(t, err)
		})
	}
}

func TestValidateEventsFilters_InvalidEventType(t *testing.T) {
	for _, et := range []string{"5", "6", "7", "-1", "14", "abc"} {
		t.Run("eventType_"+et, func(t *testing.T) {
			filters := map[string][]string{"eventType": {et}}
			err := validateEventsFilters(filters)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid eventType")
		})
	}
}

func TestValidateEventsFilters_InvalidTickNumber(t *testing.T) {
	filters := map[string][]string{"tickNumber": {"not-a-number"}}
	err := validateEventsFilters(filters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid tickNumber")
}

func TestValidateEventsFilters_UnsupportedFilter(t *testing.T) {
	filters := map[string][]string{"unknownFilter": {"value"}}
	err := validateEventsFilters(filters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported filter")
}

func TestValidateEventsFilters_TooManyFilters(t *testing.T) {
	filters := map[string][]string{
		"transactionHash": {"abc"},
		"tickNumber":      {"42"},
		"eventType":       {"1"},
		"extra":           {"value"},
	}
	err := validateEventsFilters(filters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many filters")
}

func TestValidateEventsFilters_CombinedFilters(t *testing.T) {
	filters := map[string][]string{
		"transactionHash": {"abc123"},
		"tickNumber":      {"42"},
		"eventType":       {"0"},
	}
	err := validateEventsFilters(filters)
	require.NoError(t, err)
}

func TestValidateEventsFilters_EmptyFilters(t *testing.T) {
	err := validateEventsFilters(nil)
	require.NoError(t, err)

	err = validateEventsFilters(map[string][]string{})
	require.NoError(t, err)
}
