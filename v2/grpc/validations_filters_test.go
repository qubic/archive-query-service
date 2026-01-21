package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const validId = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB"
const invalidId = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func TestValidations_validateFilters_givenAllValid_thenNoError(t *testing.T) {
	filters := map[string][]string{
		"source":              {validId},
		"destination-exclude": {validId},
		"amount":              {"100"},
		"inputType":           {"42"},
		"tickNumber":          {"43"},
	}
	err := validateIdentityTransactionQueryFilters(filters)
	require.NoError(t, err)
}

func TestValidations_validateFilters_givenMultipleValidValues_thenNoError(t *testing.T) {
	filters := map[string][]string{
		"source-exclude": {validId, validId},
		"destination":    {validId, validId},
		"amount":         {"100"},
		"inputType":      {"42"},
	}
	err := validateIdentityTransactionQueryFilters(filters)
	require.NoError(t, err)
}

func TestValidations_validateFilters_givenConflictingSourceFilter_thenError(t *testing.T) {
	filters := map[string][]string{
		"source":         {validId, validId},
		"source-exclude": {validId},
	}
	err := validateIdentityTransactionQueryFilters(filters)
	require.Error(t, err)
}

func TestValidations_validateFilters_givenConflictingDestinationFilter_thenError(t *testing.T) {
	filters := map[string][]string{
		"destination":         {validId},
		"destination-exclude": {validId, validId},
	}
	err := validateIdentityTransactionQueryFilters(filters)
	require.Error(t, err)
}

func TestValidations_validateFilters_givenUnsupported_thenError(t *testing.T) {
	filters := map[string][]string{"timestamp": {"42"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "unsupported filter: [timestamp]")
}

func TestValidations_validateFilters_givenInvalidAmount(t *testing.T) {
	filters := map[string][]string{"amount": {"-1"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid amount filter")
}

func TestValidations_validateFilters_givenMultipleAmounts(t *testing.T) {
	filters := map[string][]string{"amount": {"1", "4"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func TestValidations_validateFilters_givenEmptyAmounts(t *testing.T) {
	filters := map[string][]string{"amount": {}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func TestValidations_validateFilters_givenMultipleInputTypes(t *testing.T) {
	filters := map[string][]string{"inputType": {"1", "2"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func TestValidations_validateFilters_givenEmptyInputType(t *testing.T) {
	filters := map[string][]string{"inputType": {}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func TestValidations_validateFilters_givenMultipleTickNumbers(t *testing.T) {
	filters := map[string][]string{"tickNumber": {"1", "2"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func TestValidations_validateFilters_givenEmptyTickNumber(t *testing.T) {
	filters := map[string][]string{"tickNumber": {}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func TestValidations_validateFilters_givenInvalidSource(t *testing.T) {
	filters := map[string][]string{"source": {invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid source filter")
}

func TestValidations_validateFilters_givenInvalidDestination(t *testing.T) {
	filters := map[string][]string{"destination": {invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid destination filter")
}

func TestValidations_validateFilters_givenInvalidSourceExclude(t *testing.T) {
	filters := map[string][]string{"source-exclude": {invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid source-exclude filter")
}

func TestValidations_validateFilters_givenInvalidDestinationExclude(t *testing.T) {
	filters := map[string][]string{"destination-exclude": {invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid destination-exclude filter")
}

func TestValidations_validateFilters_givenMultipleIdValuesIncludingInvalid_thenError(t *testing.T) {
	filters := map[string][]string{"source": {validId, invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid source filter")
}

func TestValidations_validateFilters_givenInvalidInputType(t *testing.T) {
	filters := map[string][]string{"inputType": {"foo"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid inputType filter")
}

func TestValidations_validateFilters_givenEmpty(t *testing.T) {
	filters := map[string][]string{}
	err := validateIdentityTransactionQueryFilters(filters)
	require.NoError(t, err)
	err = validateIdentityTransactionQueryFilters(nil)
	require.NoError(t, err)
}
