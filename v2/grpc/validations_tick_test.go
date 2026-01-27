package grpc

import (
	"reflect"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
)

func Test_createTickFilters(t *testing.T) {
	tests := []struct {
		name    string
		filters map[string]string
		want    map[string][]string
		wantErr bool
	}{
		{
			name:    "empty filters",
			filters: map[string]string{},
			want:    map[string][]string{},
			wantErr: false,
		},
		{
			name: "single source filter",
			filters: map[string]string{
				"source": validId,
			},
			want: map[string][]string{
				"source": {validId},
			},
			wantErr: false,
		},
		{
			name: "source filter with spaces trimmed",
			filters: map[string]string{
				"source": "  " + validId + "  ",
			},
			want: map[string][]string{
				"source": {validId},
			},
			wantErr: false,
		},
		{
			name: "comma-separated values NOT split",
			filters: map[string]string{
				"source": "value1,value2",
			},
			want: map[string][]string{
				"source": {"value1,value2"},
			},
			wantErr: false,
		},
		{
			name: "multiple different filters",
			filters: map[string]string{
				"source":      validId,
				"destination": validId,
				"amount":      "100",
				"inputType":   "1",
			},
			want: map[string][]string{
				"source":      {validId},
				"destination": {validId},
				"amount":      {"100"},
				"inputType":   {"1"},
			},
			wantErr: false,
		},
		{
			name: "empty value error",
			filters: map[string]string{
				"source": "",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "whitespace-only value error",
			filters: map[string]string{
				"source": "   ",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createTickFilters(tt.filters)
			if (err != nil) != tt.wantErr {
				t.Errorf("createTickFilters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createTickFilters() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidations_validateTickTransactionQueryFilters_givenAllValid_thenNoError(t *testing.T) {
	filters := map[string][]string{
		"source":      {validId},
		"destination": {validId},
		"amount":      {"100"},
		"inputType":   {"42"},
	}
	err := validateTickTransactionQueryFilters(filters)
	require.NoError(t, err)
}

func TestValidations_validateTickTransactionQueryFilters_givenEmpty_thenNoError(t *testing.T) {
	err := validateTickTransactionQueryFilters(map[string][]string{})
	require.NoError(t, err)
	err = validateTickTransactionQueryFilters(nil)
	require.NoError(t, err)
}

func TestValidations_validateTickTransactionQueryFilters_givenTickNumber_thenError(t *testing.T) {
	filters := map[string][]string{"tickNumber": {"42"}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "unsupported filter: [tickNumber]")
}

func TestValidations_validateTickTransactionQueryFilters_givenSourceExclude_thenError(t *testing.T) {
	filters := map[string][]string{"source-exclude": {validId}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "unsupported filter: [source-exclude]")
}

func TestValidations_validateTickTransactionQueryFilters_givenDestinationExclude_thenError(t *testing.T) {
	filters := map[string][]string{"destination-exclude": {validId}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "unsupported filter: [destination-exclude]")
}

func TestValidations_validateTickTransactionQueryFilters_givenTimestamp_thenError(t *testing.T) {
	filters := map[string][]string{"timestamp": {"1234567890"}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "unsupported filter: [timestamp]")
}

func TestValidations_validateTickTransactionQueryFilters_givenMultipleSourceValues_thenError(t *testing.T) {
	filters := map[string][]string{"source": {validId, validId}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "must have exactly one value")
}

func TestValidations_validateTickTransactionQueryFilters_givenMultipleDestinationValues_thenError(t *testing.T) {
	filters := map[string][]string{"destination": {validId, validId}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "must have exactly one value")
}

func TestValidations_validateTickTransactionQueryFilters_givenInvalidSource_thenError(t *testing.T) {
	filters := map[string][]string{"source": {invalidId}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid source filter")
}

func TestValidations_validateTickTransactionQueryFilters_givenInvalidDestination_thenError(t *testing.T) {
	filters := map[string][]string{"destination": {invalidId}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid destination filter")
}

func TestValidations_validateTickTransactionQueryFilters_givenInvalidAmount_thenError(t *testing.T) {
	filters := map[string][]string{"amount": {"-1"}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid amount filter")
}

func TestValidations_validateTickTransactionQueryFilters_givenInvalidInputType_thenError(t *testing.T) {
	filters := map[string][]string{"inputType": {"foo"}}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid inputType filter")
}

func TestValidations_validateTickTransactionQueryFilters_givenTooManyFilters_thenError(t *testing.T) {
	filters := map[string][]string{
		"source":      {validId},
		"destination": {validId},
		"amount":      {"100"},
		"inputType":   {"1"},
		"extra":       {"value"},
	}
	err := validateTickTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "too many filters")
}

func TestValidations_validateTickTransactionQueryRanges_givenAllValid(t *testing.T) {
	ranges, err := validateTickTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterAmount: {
			LowerBound: &api.Range_Gte{
				Gte: "1000",
			},
			UpperBound: &api.Range_Lte{
				Lte: "10000",
			},
		},
		FilterInputType: {
			LowerBound: &api.Range_Gt{
				Gt: "0",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, ranges, 2)
}

func TestValidations_validateTickTransactionQueryRanges_givenEmpty(t *testing.T) {
	_, err := validateTickTransactionQueryRanges(map[string][]string{}, nil)
	require.NoError(t, err)
	_, err = validateTickTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{})
	require.NoError(t, err)
	_, err = validateTickTransactionQueryRanges(nil, nil)
	require.NoError(t, err)
}

func TestValidations_validateTickTransactionQueryRanges_givenTickNumber_thenError(t *testing.T) {
	_, err := validateTickTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterTickNumber: {
			LowerBound: &api.Range_Gte{
				Gte: "1",
			},
		},
	})
	require.ErrorContains(t, err, "unsupported range: [tickNumber]")
}

func TestValidations_validateTickTransactionQueryRanges_givenTimestamp_thenError(t *testing.T) {
	_, err := validateTickTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterTimestamp: {
			LowerBound: &api.Range_Gt{
				Gt: "1000000",
			},
		},
	})
	require.ErrorContains(t, err, "unsupported range: [timestamp]")
}

func TestValidations_validateTickTransactionQueryRanges_givenDuplicateFilter_thenError(t *testing.T) {
	filters := map[string][]string{FilterAmount: {"100"}}
	ranges := map[string]*api.Range{FilterAmount: nil}
	_, err := validateTickTransactionQueryRanges(filters, ranges)
	require.ErrorContains(t, err, "already declared as filter")
}

func TestValidations_validateTickTransactionQueryRanges_givenTooManyRanges_thenError(t *testing.T) {
	ranges := map[string]*api.Range{
		"amount":    {},
		"inputType": {},
		"extra":     {},
	}
	_, err := validateTickTransactionQueryRanges(map[string][]string{}, ranges)
	require.ErrorContains(t, err, "too many ranges")
}

func TestValidations_validateTickTransactionQueryRanges_givenInvalidAmountRange_thenError(t *testing.T) {
	_, err := validateTickTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterAmount: {
			LowerBound: &api.Range_Gte{
				Gte: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid amount range")
}

func TestValidations_validateTickTransactionQueryRanges_givenInvalidInputTypeRange_thenError(t *testing.T) {
	_, err := validateTickTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterInputType: {
			LowerBound: &api.Range_Gt{
				Gt: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid inputType range")
}
