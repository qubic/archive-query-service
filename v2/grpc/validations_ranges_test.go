package grpc

import (
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
)

func TestValidations_validateRanges_givenAllValid(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterAmount: {
			LowerBound: &api.Range_Gte{
				Gte: "1000",
			},
			UpperBound: &api.Range_Lte{
				Lte: "10000",
			},
		},
		FilterTickNumber: {
			LowerBound: &api.Range_Gte{
				Gte: "1",
			},
			UpperBound: &api.Range_Lt{
				Lt: "999999",
			},
		},
		FilterInputType: {
			LowerBound: &api.Range_Gt{
				Gt: "0",
			},
		},
		FilterTimestamp: {
			LowerBound: &api.Range_Gt{
				Gt: "1000000",
			},
		},
	})
	require.NoError(t, err)
}

func TestValidations_validateRanges(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string][]string{}, nil)
	require.NoError(t, err)
}

func TestValidations_validateRanges_givenUnsupported_thenError(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		"foo": {},
	})
	require.ErrorContains(t, err, "unsupported range: [foo]")
}

func TestValidations_validateRanges_EmptyRange_thenError(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterAmount: {},
	})
	require.ErrorContains(t, err, "invalid range: no bounds")
}

func TestValidations_validateRanges_givenInvalidRange_thenError(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterAmount: {
			LowerBound: &api.Range_Gte{
				Gte: "42",
			},
			UpperBound: &api.Range_Lte{
				Lte: "42",
			},
		},
	})
	require.ErrorContains(t, err, "invalid range: [42:42]")
}

func TestValidations_validateRanges_givenEmpty(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string][]string{}, nil)
	require.NoError(t, err)
	_, err = validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{})
	require.NoError(t, err)
	_, err = validateIdentityTransactionQueryRanges(nil, map[string]*api.Range{})
	require.NoError(t, err)
	_, err = validateIdentityTransactionQueryRanges(nil, nil)
	require.NoError(t, err)
}

func TestValidations_validateRanges_givenInvalidRangeValue_thenError(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterAmount: {
			LowerBound: &api.Range_Gte{
				Gte: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid amount range: invalid [gte] value")
	_, err = validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterTickNumber: {
			LowerBound: &api.Range_Gt{
				Gt: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid tickNumber range: invalid [gt] value")
	_, err = validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterTimestamp: {
			UpperBound: &api.Range_Lte{
				Lte: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid timestamp range: invalid [lte] value")
	_, err = validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterInputType: {
			UpperBound: &api.Range_Lt{
				Lt: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid inputType range: invalid [lt] value")
}

func TestValidations_validateRanges_givenDuplicateFilter_thenError(t *testing.T) {
	filters := map[string][]string{FilterAmount: {"foo"}}
	ranges := map[string]*api.Range{FilterAmount: nil}
	_, err := validateIdentityTransactionQueryRanges(filters, ranges)
	require.ErrorContains(t, err, "already declared as filter")
}

func TestValidations_validateRanges_tickNumberWithUpperAndLowerRange(t *testing.T) {
	result, err := validateIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterTickNumber: {
			LowerBound: &api.Range_Gte{
				Gte: "100",
			},
			UpperBound: &api.Range_Lte{
				Lte: "200",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, result, FilterTickNumber)

	tickRange := result[FilterTickNumber]
	require.Len(t, tickRange, 2)
	require.Equal(t, "gte", tickRange[0].Operation)
	require.Equal(t, "100", tickRange[0].Value)
	require.Equal(t, "lte", tickRange[1].Operation)
	require.Equal(t, "200", tickRange[1].Value)
}
