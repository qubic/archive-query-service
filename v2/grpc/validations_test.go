package grpc

import (
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
)

func TestValidations_validateFilters_givenAllValid(t *testing.T) {
	filters := map[string]string{
		"source":      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
		"destination": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB",
		"amount":      "100",
		"inputType":   "42",
	}
	err := validateIdentityTransactionQueryFilters(filters)
	require.NoError(t, err)
}

func TestValidations_validateFilters_givenUnsupported_thenError(t *testing.T) {
	filters := map[string]string{"tickNumber": "42"}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "unsupported filter: [tickNumber]")
}

func TestValidations_validateFilters_givenInvalidAmount(t *testing.T) {
	filters := map[string]string{"amount": "-1"}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid amount filter")
}

func TestValidations_validateFilters_givenInvalidSource(t *testing.T) {
	filters := map[string]string{"source": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid source filter")
}

func TestValidations_validateFilters_givenInvalidDestination(t *testing.T) {
	filters := map[string]string{"destination": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid destination filter")
}

func TestValidations_validateFilters_givenInvalidInputType(t *testing.T) {
	filters := map[string]string{"inputType": "foo"}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid inputType filter")
}

func TestValidations_validateFilters_givenEmpty(t *testing.T) {
	filters := map[string]string{}
	err := validateIdentityTransactionQueryFilters(filters)
	require.NoError(t, err)
	err = validateIdentityTransactionQueryFilters(nil)
	require.NoError(t, err)
}

func TestValidations_validateRanges_givenAllValid(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string]string{}, map[string]*api.Range{
		"amount": {
			LowerBound: &api.Range_Gte{
				Gte: "1000",
			},
			UpperBound: &api.Range_Lte{
				Lte: "10000",
			},
		},
		"tickNumber": {
			LowerBound: &api.Range_Gte{
				Gte: "1",
			},
			UpperBound: &api.Range_Lt{
				Lt: "999999",
			},
		},
		"inputType": {
			LowerBound: &api.Range_Gt{
				Gt: "0",
			},
		},
		"timestamp": {
			LowerBound: &api.Range_Gt{
				Gt: "1000000",
			},
		},
	})
	require.NoError(t, err)
}

func TestValidations_validateRanges(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string]string{}, nil)
	require.NoError(t, err)
}

func TestValidations_validateRanges_givenUnsupported_thenError(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string]string{}, map[string]*api.Range{
		"foo": {},
	})
	require.ErrorContains(t, err, "unsupported range: [foo]")
}

func TestValidations_validateRanges_EmptyRange_thenError(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string]string{}, map[string]*api.Range{
		"amount": {},
	})
	require.ErrorContains(t, err, "invalid range: no bounds")
}

func TestValidations_validateRanges_givenInvalidRange_thenError(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string]string{}, map[string]*api.Range{
		"amount": {
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
	_, err := validateIdentityTransactionQueryRanges(map[string]string{}, nil)
	require.NoError(t, err)
	_, err = validateIdentityTransactionQueryRanges(map[string]string{}, map[string]*api.Range{})
	require.NoError(t, err)
	_, err = validateIdentityTransactionQueryRanges(nil, map[string]*api.Range{})
	require.NoError(t, err)
	_, err = validateIdentityTransactionQueryRanges(nil, nil)
	require.NoError(t, err)
}

func TestValidations_validateRanges_givenInvalidRangeValue_thenError(t *testing.T) {
	_, err := validateIdentityTransactionQueryRanges(map[string]string{}, map[string]*api.Range{
		"amount": {
			LowerBound: &api.Range_Gte{
				Gte: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid amount range: invalid [gte] value")
	_, err = validateIdentityTransactionQueryRanges(map[string]string{}, map[string]*api.Range{
		"amount": {
			LowerBound: &api.Range_Gt{
				Gt: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid amount range: invalid [gt] value")
	_, err = validateIdentityTransactionQueryRanges(map[string]string{}, map[string]*api.Range{
		"amount": {
			UpperBound: &api.Range_Lte{
				Lte: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid amount range: invalid [lte] value")
	_, err = validateIdentityTransactionQueryRanges(map[string]string{}, map[string]*api.Range{
		"amount": {
			UpperBound: &api.Range_Lt{
				Lt: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid amount range: invalid [lt] value")
}

func TestValidations_validateRanges_givenDuplicateFilter_thenError(t *testing.T) {
	filters := map[string]string{"amount": "foo"}
	ranges := map[string]*api.Range{"amount": nil}
	_, err := validateIdentityTransactionQueryRanges(filters, ranges)
	require.ErrorContains(t, err, "already declared as filter")
}
