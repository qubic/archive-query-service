package api

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func Test_GetCacheKeyIdenticalRequestSameKey(t *testing.T) {
	first := GetTransactionsForIdentityRequest{
		Filters: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
		Ranges: map[string]*Range{
			"1": {
				LowerBound: &Range_Gt{Gt: "100"},
				UpperBound: &Range_Lt{Lt: "200"},
			},
			"2": {
				LowerBound: &Range_Gt{Gt: "110"},
				UpperBound: &Range_Lt{Lt: "220"},
			},
		},
	}

	firstSerialized, err := first.GetCacheKey()
	require.NoError(t, err, "getting cache key for first request")

	second := GetTransactionsForIdentityRequest{
		Filters: map[string]string{
			"key2": "value2",
			"key1": "value1",
		},
		Ranges: map[string]*Range{
			"2": {
				UpperBound: &Range_Lt{Lt: "220"},
				LowerBound: &Range_Gt{Gt: "110"},
			},
			"1": {
				UpperBound: &Range_Lt{Lt: "200"},
				LowerBound: &Range_Gt{Gt: "100"},
			},
		},
	}

	secondSerialized, err := second.GetCacheKey()
	require.NoError(t, err, "getting cache key for second request")

	diff := cmp.Diff(firstSerialized, secondSerialized)
	require.Empty(t, diff, "serialized requests should be equal, got diff: %s", diff)
}

func Test_GetCacheKeyNotIdenticalRequestDifferentKey(t *testing.T) {
	first := GetTransactionsForIdentityRequest{
		Filters: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
		Ranges: map[string]*Range{
			"1": {
				LowerBound: &Range_Gt{Gt: "100"},
				UpperBound: &Range_Lt{Lt: "200"},
			},
			"2": {
				LowerBound: &Range_Gt{Gt: "110"},
				UpperBound: &Range_Lt{Lt: "220"},
			},
		},
	}

	firstSerialized, err := first.GetCacheKey()
	require.NoError(t, err, "getting cache key for first request")

	second := GetTransactionsForIdentityRequest{
		Filters: map[string]string{
			"key1": "value1",
			"key3": "value3",
		},
		Ranges: map[string]*Range{
			"1": {
				LowerBound: &Range_Gt{Gt: "100"},
				UpperBound: &Range_Lt{Lt: "200"},
			},
			"2": {
				LowerBound: &Range_Gt{Gt: "110"},
				UpperBound: &Range_Lt{Lt: "220"},
			},
		},
	}

	secondSerialized, err := second.GetCacheKey()
	require.NoError(t, err, "getting cache key for second request")

	diff := cmp.Diff(firstSerialized, secondSerialized)
	require.NotEmpty(t, diff, "serialized requests should not be equal")
}
