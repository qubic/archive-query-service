package filters

import (
	"fmt"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validTransactionHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaafxib"
const validId4 = "FAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAYWJB"
const validId5 = "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQGNM"

func TestCreateEventsFilters_ValidFilters(t *testing.T) {
	filters := map[string]string{
		"transactionHash": validTransactionHash,
		"tickNumber":      "42",
		"logType":         "1",
		"logId":           "2",
		"categories":      "4",
	}
	result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Equal(t, map[string][]string{
		"transactionHash": {validTransactionHash},
		"tickNumber":      {"42"},
		"logType":         {"1"},
		"logId":           {"2"},
		"categories":      {"4"},
	}, result)
}

func TestCreateEventsFilters_EmptyValue(t *testing.T) {
	filters := map[string]string{
		"transactionHash": "",
	}
	_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty value")
}

func TestCreateEventsFilters_MultipleValues(t *testing.T) {
	filters := map[string]string{
		"source": fmt.Sprintf(" %s, %s  ,%s  ", validId, validId2, validId3),
	}
	created, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Len(t, created["source"], 3)
	assert.Equal(t, created["source"], []string{validId, validId2, validId3})
}

func TestCreateEventsFilters_InvalidIdentity_Error(t *testing.T) {
	filters := map[string]string{
		"source": fmt.Sprintf("%s, %s", validId, invalidId),
	}
	_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid identity")
}

func TestValidateEventsFilters_ValidTransactionHash(t *testing.T) {
	filters := map[string][]string{"transactionHash": {validTransactionHash}}
	err := validateEventsFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
}

func TestValidateEventsFilters_ValidTickNumber(t *testing.T) {
	filters := map[string][]string{"tickNumber": {"42"}}
	err := validateEventsFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
}

func TestCreateEventsFilters_SupportMultipleLogTypes(t *testing.T) {
	inputLogTypes := "1,2,3,4,5"
	filters, err := CreateEventFilters(map[string]string{"logType": inputLogTypes}, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Len(t, filters["logType"], 5)
	assert.Contains(t, filters["logType"], "1", "2", "3", "4", "5")
}

func TestCreateEventsFilters_SupportMultipleCategories(t *testing.T) {
	categories := "1,2,3,4,5"
	filters, err := CreateEventFilters(map[string]string{"categories": categories}, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Len(t, filters["categories"], 5)
	assert.Contains(t, filters["categories"], "1", "2", "3", "4", "5")
}

func TestValidateEventsFilters_ValidLogType(t *testing.T) {
	for _, et := range []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "255"} {
		t.Run("logType_"+et, func(t *testing.T) {
			filters := map[string][]string{"logType": {et}}
			err := validateEventsFilters(filters, AllowedEventIncludeFilters)
			require.NoError(t, err)
		})
	}
}

func TestValidateEventsFilters_InvalidLogType(t *testing.T) {
	for _, et := range []string{"-1", "256", "abc"} {
		t.Run("logType_"+et, func(t *testing.T) {
			filters := map[string][]string{"logType": {et}}
			err := validateEventsFilters(filters, AllowedEventIncludeFilters)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid [logType] filter")
		})
	}
}

func TestValidateEventsFilters_InvalidTickNumber(t *testing.T) {
	filters := map[string][]string{"tickNumber": {"not-a-number"}}
	err := validateEventsFilters(filters, AllowedEventIncludeFilters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid [tickNumber] filter")
}

func TestValidateEventsFilters_UnsupportedFilter(t *testing.T) {
	filters := map[string][]string{"unknownFilter": {"value"}}
	err := validateEventsFilters(filters, map[string]bool{"foo": true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported filter")
}

func TestValidateEventsFilters_TooManyFilters(t *testing.T) {
	filters := map[string][]string{
		"transactionHash": {"abc"},
		"tickNumber":      {"42"},
		"logType":         {"1"},
	}
	err := validateEventsFilters(filters, map[string]bool{"foo": true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many filters")
}

func TestValidateEventsFilters_CombinedFilters(t *testing.T) {
	filters := map[string][]string{
		"transactionHash": {validTransactionHash},
		"tickNumber":      {"42"},
		"logType":         {"0"},
	}
	err := validateEventsFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
}

func TestValidateEventsFilters_EmptyFilters(t *testing.T) {
	err := validateEventsFilters(nil, map[string]bool{})
	require.NoError(t, err)

	err = validateEventsFilters(map[string][]string{}, map[string]bool{})
	require.NoError(t, err)
}

// tests for source, destination, epoch, and amount filters

func TestCreateEventsFilters_IdentityFilters_SingleValue(t *testing.T) {
	filterNames := []string{"source", "destination"}
	for _, filterName := range filterNames {
		t.Run(filterName, func(t *testing.T) {
			filters := map[string]string{
				filterName: validId,
			}
			result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
			require.NoError(t, err)
			assert.Equal(t, []string{validId}, result[filterName])
		})
	}
}

func TestCreateEventsFilters_IdentityFilters_MultipleValues(t *testing.T) {
	filterNames := []string{"source", "destination"}
	for _, filterName := range filterNames {
		t.Run(filterName, func(t *testing.T) {
			filters := map[string]string{
				filterName: fmt.Sprintf("%s,%s,%s", validId, validId2, validId3),
			}
			result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
			require.NoError(t, err)
			assert.Equal(t, []string{validId, validId2, validId3}, result[filterName])
		})
	}
}

func TestCreateEventsFilters_IdentityFilters_MaxValues(t *testing.T) {
	filterNames := []string{"source", "destination"}
	for _, filterName := range filterNames {
		t.Run(filterName, func(t *testing.T) {
			filters := map[string]string{
				filterName: fmt.Sprintf("%s,%s,%s,%s", validId, validId2, validId3, validId4),
			}
			result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
			require.NoError(t, err)
			assert.Equal(t, []string{validId, validId2, validId3, validId4}, result[filterName])
		})
	}
}

func TestCreateEventsFilters_IdentityFilters_TooManyValues(t *testing.T) {
	filterNames := []string{"source", "destination"}
	for _, filterName := range filterNames {
		t.Run(filterName, func(t *testing.T) {
			filters := map[string]string{
				filterName: fmt.Sprintf("%s,%s,%s,%s,%s,%s", validId, validId2, validId3, validId4, validId5, validId),
			}
			_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "exceeds maximum length")
		})
	}
}

func TestCreateEventsFilters_IdentityFilters_EmptyValue(t *testing.T) {
	filterNames := []string{"source", "destination"}
	for _, filterName := range filterNames {
		t.Run(filterName, func(t *testing.T) {
			filters := map[string]string{
				filterName: "",
			}
			_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "empty value")
		})
	}
}

func TestCreateEventsFilters_IdentityFilters_DuplicateValues(t *testing.T) {
	filterNames := []string{"source", "destination"}
	for _, filterName := range filterNames {
		t.Run(filterName, func(t *testing.T) {
			filters := map[string]string{
				filterName: fmt.Sprintf("%s,%s,%s", validId, validId2, validId),
			}
			_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "duplicate value")
		})
	}
}

func TestCreateEventsFilters_IdentityFilters_InvalidIdentity(t *testing.T) {
	filterNames := []string{"source", "destination"}
	for _, filterName := range filterNames {
		t.Run(filterName, func(t *testing.T) {
			filters := map[string]string{
				filterName: invalidId,
			}
			_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid identity")
		})
	}
}

func TestCreateEventsFilters_IdentityFilters_EmptyInList(t *testing.T) {
	filterNames := []string{"source", "destination"}
	for _, filterName := range filterNames {
		t.Run(filterName, func(t *testing.T) {
			filters := map[string]string{
				filterName: fmt.Sprintf("%s,,%s", validId, validId2),
			}
			_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "empty value")
		})
	}
}

func TestCreateEventsFilters_Epoch_ValidValue(t *testing.T) {
	filters := map[string]string{
		"epoch": "100",
	}
	result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Equal(t, []string{"100"}, result["epoch"])
}

func TestCreateEventsFilters_Epoch_ZeroValue(t *testing.T) {
	filters := map[string]string{
		"epoch": "0",
	}
	result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Equal(t, []string{"0"}, result["epoch"])
}

func TestCreateEventsFilters_Epoch_InvalidNegative(t *testing.T) {
	filters := map[string]string{
		"epoch": "-1",
	}
	_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid [epoch] filter")
}

func TestCreateEventsFilters_Epoch_InvalidString(t *testing.T) {
	filters := map[string]string{
		"epoch": "abc",
	}
	_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid [epoch] filter")
}

func TestCreateEventsFilters_Amount_ValidValue(t *testing.T) {
	filters := map[string]string{
		"amount": "1000",
	}
	result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Equal(t, []string{"1000"}, result["amount"])
}

func TestCreateEventsFilters_Amount_InvalidNegative(t *testing.T) {
	filters := map[string]string{
		"amount": "-100",
	}
	_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid [amount] filter")
}

func TestCreateEventsFilters_Amount_InvalidString(t *testing.T) {
	filters := map[string]string{
		"amount": "not-a-number",
	}
	_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid [amount] filter")
}

func TestCreateEventsFilters_NumberOfShares_ValidValue(t *testing.T) {
	filters := map[string]string{
		"numberOfShares": "1000",
	}
	result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Equal(t, []string{"1000"}, result["numberOfShares"])
}

func TestCreateEventsFilters_CombinedSourceAndDestination(t *testing.T) {
	filters := map[string]string{
		"source":      fmt.Sprintf("%s,%s", validId, validId2),
		"destination": validId3,
	}
	result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Equal(t, []string{validId, validId2}, result["source"])
	assert.Equal(t, []string{validId3}, result["destination"])
}

func TestCreateEventsFilters_MaxLengthForIdentityFilters(t *testing.T) {
	// 5 IDs (60 chars each) + 4 commas + 4 spaces = 309 chars total
	longValue := fmt.Sprintf("%s,%s,%s,%s", validId, validId2, validId3, validId4)
	filters := map[string]string{
		"source": longValue,
	}
	result, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.NoError(t, err)
	assert.Len(t, result["source"], 4)
}

func TestCreateEventsFilters_ExceedsMaxLengthForIdentityFilters(t *testing.T) {
	// Create a string that exceeds 309 characters
	longValue := fmt.Sprintf("%s,%s,%s,%s,%s,%s", validId, validId2, validId3, validId4, validId5, validId)
	filters := map[string]string{
		"source": longValue,
	}
	_, err := CreateEventFilters(filters, AllowedEventIncludeFilters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum length")
}

func TestCheckForConflictingFilters(t *testing.T) {
	includeFilters := map[string][]string{
		"source": {"value1"},
	}
	excludeFilters := map[string][]string{
		"source": {"value2"},
	}
	conflictingFilters := entities.Filters{Include: includeFilters, Exclude: excludeFilters}
	err := VerifyNoConflictingFilters(conflictingFilters)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate [source] filter")

	// no conflict
	nonConflictingFilters := entities.Filters{Include: includeFilters, Exclude: map[string][]string{"foo": {"bar"}}}
	err = VerifyNoConflictingFilters(nonConflictingFilters)
	require.NoError(t, err)
}

func TestCreateEventQueryRanges_ValidRange(t *testing.T) {
	ranges := map[string]*api.Range{
		"amount": {
			LowerBound: &api.Range_Gte{Gte: "100"},
			UpperBound: &api.Range_Lte{Lte: "1000"},
		},
	}
	result, err := CreateEventRanges(ranges, AllowedEventRanges)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Contains(t, result, "amount")
	assert.Len(t, result["amount"], 2)
	assert.Equal(t, "gte", result["amount"][0].Operation)
	assert.Equal(t, "100", result["amount"][0].Value)
	assert.Equal(t, "lte", result["amount"][1].Operation)
	assert.Equal(t, "1000", result["amount"][1].Value)
}

func TestCreateEventQueryRanges_UnsupportedRangeType(t *testing.T) {
	ranges := map[string]*api.Range{
		"logType": {
			LowerBound: &api.Range_Gt{Gt: "1"},
			UpperBound: &api.Range_Lte{Lte: "6"},
		},
	}
	_, err := CreateEventRanges(ranges, AllowedEventRanges)
	require.ErrorContains(t, err, "unsupported filter [logType]")
}

func TestCreateEventQueryRanges_InvalidRangeBounds(t *testing.T) {
	ranges := map[string]*api.Range{
		"numberOfShares": {
			LowerBound: &api.Range_Gte{Gte: "100"},
			UpperBound: &api.Range_Lte{Lte: "20"},
		},
	}
	_, err := CreateEventRanges(ranges, AllowedEventRanges)
	require.ErrorContains(t, err, "invalid [numberOfShares] range")
}

