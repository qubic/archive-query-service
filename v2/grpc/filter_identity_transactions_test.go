package grpc

import (
	"fmt"
	"reflect"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
)

const validId = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB"
const validId2 = "BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID"
const validId3 = "EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVWRF"
const invalidId = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func Test_createIdentityTransactionFilters(t *testing.T) {
	tests := []struct {
		name    string
		filters map[string]string
		want    map[string][]string
		wantErr bool
	}{
		{
			name: "single value for source (trim)",
			filters: map[string]string{
				"source": validId + " ",
			},
			want: map[string][]string{
				"source": {validId},
			},
			wantErr: false,
		},
		{
			name: "multiple values for source (split)",
			filters: map[string]string{
				"source": fmt.Sprintf("%s,%s ,%s", validId, validId2, validId3),
			},
			want: map[string][]string{
				"source": {validId, validId2, validId3},
			},
			wantErr: false,
		},
		{
			name: "duplicate value",
			filters: map[string]string{
				"source": fmt.Sprintf("%s,%s,%s", validId, validId2, validId),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "multiple values for other (error because of comma)",
			filters: map[string]string{
				"other": " value1, value2 ",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createIdentityTransactionFilters(tt.filters)
			if (err != nil) != tt.wantErr {
				t.Errorf("createIdentityTransactionFilters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createIdentityTransactionFilters() got = %v, want %v", got, tt.want)
			}
		})
	}

}

// validations do no duplicate checking. creating filters does both crating and validating

func Test_validateIdentityTransactionFilters_givenAllValid_thenNoError(t *testing.T) {
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

func Test_validateIdentityTransactionFilters_givenMultipleValidValues_thenNoError(t *testing.T) {
	filters := map[string][]string{
		"source-exclude": {validId, validId},
		"destination":    {validId, validId},
		"amount":         {"100"},
		"inputType":      {"42"},
	}
	err := validateIdentityTransactionQueryFilters(filters)
	require.NoError(t, err)
}

func Test_validateIdentityTransactionFilters_givenConflictingSourceFilter_thenError(t *testing.T) {
	filters := map[string][]string{
		"source":         {validId, validId},
		"source-exclude": {validId},
	}
	err := validateIdentityTransactionQueryFilters(filters)
	require.Error(t, err)
}

func Test_validateIdentityTransactionFilters_givenConflictingDestinationFilter_thenError(t *testing.T) {
	filters := map[string][]string{
		"destination":         {validId},
		"destination-exclude": {validId, validId},
	}
	err := validateIdentityTransactionQueryFilters(filters)
	require.Error(t, err)
}

func Test_validateIdentityTransactionFilters_givenUnsupported_thenError(t *testing.T) {
	filters := map[string][]string{"timestamp": {"42"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "unsupported filter: [timestamp]")
}

func Test_validateIdentityTransactionFilters_givenInvalidAmount(t *testing.T) {
	filters := map[string][]string{"amount": {"-1"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid numeric value")
}

func Test_validateIdentityTransactionFilters_givenMultipleAmounts(t *testing.T) {
	filters := map[string][]string{"amount": {"1", "4"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func Test_validateIdentityTransactionFilters_givenEmptyAmounts(t *testing.T) {
	filters := map[string][]string{"amount": {}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func Test_validateIdentityTransactionFilters_givenMultipleInputTypes(t *testing.T) {
	filters := map[string][]string{"inputType": {"1", "2"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func Test_validateIdentityTransactionFilters_givenEmptyInputType(t *testing.T) {
	filters := map[string][]string{"inputType": {}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func Test_validateIdentityTransactionFilters_givenMultipleTickNumbers(t *testing.T) {
	filters := map[string][]string{"tickNumber": {"1", "2"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func Test_validateIdentityTransactionFilters_givenEmptyTickNumber(t *testing.T) {
	filters := map[string][]string{"tickNumber": {}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid number of values")
}

func Test_validateIdentityTransactionFilters_givenInvalidSource(t *testing.T) {
	filters := map[string][]string{"source": {invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid [source] filter")
}

func Test_validateIdentityTransactionFilters_givenInvalidDestination(t *testing.T) {
	filters := map[string][]string{"destination": {invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid [destination] filter")
}

func Test_validateIdentityTransactionFilters_givenInvalidSourceExclude(t *testing.T) {
	filters := map[string][]string{"source-exclude": {invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid [source-exclude] filter")
}

func Test_validateIdentityTransactionFilters_givenInvalidDestinationExclude(t *testing.T) {
	filters := map[string][]string{"destination-exclude": {invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid [destination-exclude] filter")
}

func Test_validateIdentityTransactionFilters_givenMultipleIdValuesIncludingInvalid_thenError(t *testing.T) {
	filters := map[string][]string{"source": {validId, invalidId}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid [source] filter")
}

func Test_validateIdentityTransactionFilters_givenInvalidInputType(t *testing.T) {
	filters := map[string][]string{"inputType": {"foo"}}
	err := validateIdentityTransactionQueryFilters(filters)
	require.ErrorContains(t, err, "invalid [inputType] filter")
}

func Test_validateIdentityTransactionFilters_givenEmpty(t *testing.T) {
	filters := map[string][]string{}
	err := validateIdentityTransactionQueryFilters(filters)
	require.NoError(t, err)
	err = validateIdentityTransactionQueryFilters(nil)
	require.NoError(t, err)
}

// ranges

func Test_createIdentityTransactionRanges_givenAllValid(t *testing.T) {
	_, err := createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
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

func Test_createIdentityTransactionRanges(t *testing.T) {
	_, err := createIdentityTransactionQueryRanges(map[string][]string{}, nil)
	require.NoError(t, err)
}

func Test_createIdentityTransactionRanges_givenUnsupported_thenError(t *testing.T) {
	_, err := createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		"foo": {},
	})
	require.ErrorContains(t, err, "unsupported range: [foo]")
}

func Test_createIdentityTransactionRanges_EmptyRange_thenError(t *testing.T) {
	_, err := createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterAmount: {},
	})
	require.ErrorContains(t, err, "invalid range: no bounds")
}

func Test_createIdentityTransactionRanges_givenInvalidRange_thenError(t *testing.T) {
	_, err := createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
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

func Test_createIdentityTransactionRanges_givenEmpty(t *testing.T) {
	_, err := createIdentityTransactionQueryRanges(map[string][]string{}, nil)
	require.NoError(t, err)
	_, err = createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{})
	require.NoError(t, err)
	_, err = createIdentityTransactionQueryRanges(nil, map[string]*api.Range{})
	require.NoError(t, err)
	_, err = createIdentityTransactionQueryRanges(nil, nil)
	require.NoError(t, err)
}

func Test_createIdentityTransactionRanges_givenInvalidRangeValue_thenError(t *testing.T) {
	_, err := createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterAmount: {
			LowerBound: &api.Range_Gte{
				Gte: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid amount range: invalid [gte] value")
	_, err = createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterTickNumber: {
			LowerBound: &api.Range_Gt{
				Gt: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid tickNumber range: invalid [gt] value")
	_, err = createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterTimestamp: {
			UpperBound: &api.Range_Lte{
				Lte: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid timestamp range: invalid [lte] value")
	_, err = createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
		FilterInputType: {
			UpperBound: &api.Range_Lt{
				Lt: "foo",
			},
		},
	})
	require.ErrorContains(t, err, "invalid inputType range: invalid [lt] value")
}

func Test_createIdentityTransactionRanges_givenDuplicateFilter_thenError(t *testing.T) {
	filters := map[string][]string{FilterAmount: {"foo"}}
	ranges := map[string]*api.Range{FilterAmount: nil}
	_, err := createIdentityTransactionQueryRanges(filters, ranges)
	require.ErrorContains(t, err, "is already declared")
}

func Test_createIdentityTransactionRanges_tickNumberWithUpperAndLowerRange(t *testing.T) {
	result, err := createIdentityTransactionQueryRanges(map[string][]string{}, map[string]*api.Range{
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
