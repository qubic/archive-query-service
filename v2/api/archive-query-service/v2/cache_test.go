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

func Test_GetTransactionsForTickRequest_GetCacheKey_NoFilters(t *testing.T) {
	req := GetTransactionsForTickRequest{
		TickNumber: 12345,
	}

	key, err := req.GetCacheKey()
	require.NoError(t, err)
	require.Equal(t, "ttfr:12345", key, "simple key format for requests without filters")
}

func Test_GetTransactionsForTickRequest_GetCacheKey_WithFilters(t *testing.T) {
	req := GetTransactionsForTickRequest{
		TickNumber: 12345,
		Filters: map[string]string{
			"source": "SOMEIDENTITY",
		},
	}

	key, err := req.GetCacheKey()
	require.NoError(t, err)
	require.Contains(t, key, "ttfr:", "key should have correct prefix")
	require.NotEqual(t, "ttfr:12345", key, "key should use hash when filters present")
}

func Test_GetTransactionsForTickRequest_GetCacheKey_WithRanges(t *testing.T) {
	req := GetTransactionsForTickRequest{
		TickNumber: 12345,
		Ranges: map[string]*Range{
			"amount": {
				LowerBound: &Range_Gte{Gte: "1000"},
			},
		},
	}

	key, err := req.GetCacheKey()
	require.NoError(t, err)
	require.Contains(t, key, "ttfr:", "key should have correct prefix")
	require.NotEqual(t, "ttfr:12345", key, "key should use hash when ranges present")
}

func Test_GetTransactionsForTickRequest_GetCacheKey_IdenticalRequestsSameKey(t *testing.T) {
	first := GetTransactionsForTickRequest{
		TickNumber: 42,
		Filters: map[string]string{
			"source": "ID1",
			"amount": "100",
		},
		Ranges: map[string]*Range{
			"inputType": {
				LowerBound: &Range_Gt{Gt: "0"},
			},
		},
	}

	second := GetTransactionsForTickRequest{
		TickNumber: 42,
		Filters: map[string]string{
			"amount": "100",
			"source": "ID1",
		},
		Ranges: map[string]*Range{
			"inputType": {
				LowerBound: &Range_Gt{Gt: "0"},
			},
		},
	}

	firstKey, err := first.GetCacheKey()
	require.NoError(t, err)

	secondKey, err := second.GetCacheKey()
	require.NoError(t, err)

	require.Equal(t, firstKey, secondKey, "identical requests should have same cache key")
}

func Test_GetTransactionsForTickRequest_GetCacheKey_DifferentRequestsDifferentKey(t *testing.T) {
	first := GetTransactionsForTickRequest{
		TickNumber: 42,
		Filters: map[string]string{
			"source": "ID1",
		},
	}

	second := GetTransactionsForTickRequest{
		TickNumber: 42,
		Filters: map[string]string{
			"source": "ID2",
		},
	}

	firstKey, err := first.GetCacheKey()
	require.NoError(t, err)

	secondKey, err := second.GetCacheKey()
	require.NoError(t, err)

	require.NotEqual(t, firstKey, secondKey, "different requests should have different cache keys")
}
