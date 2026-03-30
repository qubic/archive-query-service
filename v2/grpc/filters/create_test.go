package filters

import (
	"reflect"
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shared test constants
const validId = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB"
const validId2 = "BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID"
const validId3 = "EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVWRF"
const invalidId = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func TestCreateFilters(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		maxValues  int
		maxLength  int
		want       []string
		wantErr    bool
		errMessage string
	}{
		{
			name:      "single value",
			value:     "value1",
			maxValues: 1,
			maxLength: 0,
			want:      []string{"value1"},
			wantErr:   false,
		},
		{
			name:      "single value with spaces",
			value:     "  value1  ",
			maxValues: 1,
			maxLength: 0,
			want:      []string{"value1"},
			wantErr:   false,
		},
		{
			name:      "multiple values",
			value:     "value1,value2,value3",
			maxValues: 5,
			maxLength: 0,
			want:      []string{"value1", "value2", "value3"},
			wantErr:   false,
		},
		{
			name:      "multiple values with spaces",
			value:     " value1 , value2,  value3 ",
			maxValues: 5,
			maxLength: 0,
			want:      []string{"value1", "value2", "value3"},
			wantErr:   false,
		},
		{
			name:       "exceeds maximum length",
			value:      "1234567890",
			maxValues:  1,
			maxLength:  5,
			want:       nil,
			wantErr:    true,
			errMessage: "exceeds maximum length",
		},
		{
			name:       "empty value when splitting",
			value:      "value1,,value2",
			maxValues:  5,
			maxLength:  0,
			want:       nil,
			wantErr:    true,
			errMessage: "splitting values",
		},
		{
			name:       "too many values",
			value:      "v1,v2,v3,v4,v5,v6",
			maxValues:  5,
			maxLength:  0,
			want:       nil,
			wantErr:    true,
			errMessage: "splitting values",
		},
		{
			name:       "duplicate values",
			value:      "value1,value2,value1",
			maxValues:  5,
			maxLength:  0,
			want:       nil,
			wantErr:    true,
			errMessage: "splitting values",
		},
		{
			name:       "empty trimmed value",
			value:      "  ",
			maxValues:  1,
			maxLength:  0,
			want:       nil,
			wantErr:    true,
			errMessage: "trimming value",
		},
		{
			name:      "at maximum length",
			value:     "12345",
			maxValues: 1,
			maxLength: 5,
			want:      []string{"12345"},
			wantErr:   false,
		},
		{
			name:      "no max length check",
			value:     "very long value that would exceed limits",
			maxValues: 1,
			maxLength: 0,
			want:      []string{"very long value that would exceed limits"},
			wantErr:   false,
		},
		{
			name:      "max values minus one",
			value:     "v1,v2,v3,v4",
			maxValues: 5,
			want:      []string{"v1", "v2", "v3", "v4"},
			wantErr:   false,
		},
		{
			name:       "many values - exactly at limit",
			value:      "v1,v2,v3,v4,v5",
			maxValues:  5,
			want:       []string{"v1", "v2", "v3", "v4", "v5"},
			wantErr:    false,
			errMessage: "has more than [5] values",
		},
		{
			name:       "too many values - exceeds limit",
			value:      "v1,v2,v3,v4,v5,v6",
			maxValues:  5,
			want:       nil,
			wantErr:    true,
			errMessage: "has more than [5] values",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateFilters(tt.value, tt.maxValues, tt.maxLength)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateFilters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateFilters() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_splitFilterValue(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		maxValues  int
		want       []string
		wantErr    bool
		errMessage string
	}{
		{
			name:      "single value",
			value:     "value1",
			maxValues: 5,
			want:      []string{"value1"},
			wantErr:   false,
		},
		{
			name:      "multiple values",
			value:     "value1,value2,value3",
			maxValues: 5,
			want:      []string{"value1", "value2", "value3"},
			wantErr:   false,
		},
		{
			name:      "values with spaces",
			value:     " value1 , value2,  value3 ",
			maxValues: 5,
			want:      []string{"value1", "value2", "value3"},
			wantErr:   false,
		},
		{
			name:       "empty value after split",
			value:      "value1,,value2",
			maxValues:  5,
			want:       nil,
			wantErr:    true,
			errMessage: "contains empty value",
		},
		{
			name:       "empty value at start",
			value:      ",value1,value2",
			maxValues:  5,
			want:       nil,
			wantErr:    true,
			errMessage: "contains empty value",
		},
		{
			name:       "empty value at end",
			value:      "value1,value2,",
			maxValues:  5,
			want:       nil,
			wantErr:    true,
			errMessage: "contains empty value",
		},
		{
			name:       "duplicate values",
			value:      "value1,value2,value1",
			maxValues:  5,
			want:       nil,
			wantErr:    true,
			errMessage: "contains duplicate value [value1]",
		},
		{
			name:       "duplicate values with different spacing",
			value:      "value1, value2 ,value1",
			maxValues:  5,
			want:       nil,
			wantErr:    true,
			errMessage: "contains duplicate value [value1]",
		},
		{
			name:       "all empty values",
			value:      ",,",
			maxValues:  5,
			want:       nil,
			wantErr:    true,
			errMessage: "contains empty value",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitFilterValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("splitFilterValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitFilterValue() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_trimFilterValue(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		want       []string
		wantErr    bool
		errMessage string
	}{
		{
			name:    "simple value",
			value:   "value1",
			want:    []string{"value1"},
			wantErr: false,
		},
		{
			name:    "value with leading spaces",
			value:   "  value1",
			want:    []string{"value1"},
			wantErr: false,
		},
		{
			name:    "value with trailing spaces",
			value:   "value1  ",
			want:    []string{"value1"},
			wantErr: false,
		},
		{
			name:    "value with both leading and trailing spaces",
			value:   "  value1  ",
			want:    []string{"value1"},
			wantErr: false,
		},
		{
			name:    "value with internal spaces",
			value:   "value 1",
			want:    []string{"value 1"},
			wantErr: false,
		},
		{
			name:    "value with commas (not split)",
			value:   "value1,value2,value3",
			want:    []string{"value1,value2,value3"},
			wantErr: false,
		},
		{
			name:       "empty string",
			value:      "",
			want:       nil,
			wantErr:    true,
			errMessage: "empty value",
		},
		{
			name:       "only spaces",
			value:      "   ",
			want:       nil,
			wantErr:    true,
			errMessage: "empty value",
		},
		{
			name:       "only tabs",
			value:      "\t\t",
			want:       nil,
			wantErr:    true,
			errMessage: "empty value",
		},
		{
			name:       "mixed whitespace",
			value:      " \t \n ",
			want:       nil,
			wantErr:    true,
			errMessage: "empty value",
		},
		{
			name:    "single character",
			value:   "a",
			want:    []string{"a"},
			wantErr: false,
		},
		{
			name:    "numeric value",
			value:   "12345",
			want:    []string{"12345"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := trimFilterValue(tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("trimFilterValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("trimFilterValue() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_splitIncludeExcludeFilters(t *testing.T) {
	tests := []struct {
		name        string
		filters     map[string]string
		wantInclude map[string]string
		wantExclude map[string]string
	}{
		{
			name:        "empty filters",
			filters:     map[string]string{},
			wantInclude: map[string]string{},
			wantExclude: map[string]string{},
		},
		{
			name: "only include filters",
			filters: map[string]string{
				"x": "a",
				"y": "b",
			},
			wantInclude: map[string]string{
				"x": "a",
				"y": "b",
			},
			wantExclude: map[string]string{},
		},
		{
			name: "only exclude filters",
			filters: map[string]string{
				"x-exclude": "a",
				"y-exclude": "b",
			},
			wantInclude: map[string]string{},
			wantExclude: map[string]string{
				"x": "a",
				"y": "b",
			},
		},
		{
			name: "mixed filters",
			filters: map[string]string{
				"x":         "a",
				"x-exclude": "b",
				"y":         "c",
				"z-exclude": "d",
			},
			wantInclude: map[string]string{
				"x": "a",
				"y": "c",
			},
			wantExclude: map[string]string{
				"x": "b",
				"z": "d",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotInclude, gotExclude := SplitDeprecatedIncludeExcludeFilters(tt.filters)
			require.Equal(t, tt.wantInclude, gotInclude)
			require.Equal(t, tt.wantExclude, gotExclude)
		})
	}
}

func TestCreateSignedNumericRange(t *testing.T) {
	tests := []struct {
		name    string
		r       *api.Range
		bitSize int
		want    []entities.Range
		wantErr bool
	}{
		{
			name: "valid range gte and lte",
			r: &api.Range{
				LowerBound: &api.Range_Gte{Gte: "10"},
				UpperBound: &api.Range_Lte{Lte: "20"},
			},
			bitSize: 64,
			want: []entities.Range{
				{Operation: "gte", Value: "10"},
				{Operation: "lte", Value: "20"},
			},
			wantErr: false,
		},
		{
			name: "valid range gt and lt",
			r: &api.Range{
				LowerBound: &api.Range_Gt{Gt: "10"},
				UpperBound: &api.Range_Lt{Lt: "20"},
			},
			bitSize: 64,
			want: []entities.Range{
				{Operation: "gt", Value: "10"},
				{Operation: "lt", Value: "20"},
			},
			wantErr: false,
		},
		{
			name: "negative values",
			r: &api.Range{
				LowerBound: &api.Range_Gte{Gte: "-20"},
				UpperBound: &api.Range_Lte{Lte: "-10"},
			},
			bitSize: 64,
			want: []entities.Range{
				{Operation: "gte", Value: "-20"},
				{Operation: "lte", Value: "-10"},
			},
			wantErr: false,
		},
		{
			name: "invalid range (lower > upper)",
			r: &api.Range{
				LowerBound: &api.Range_Gte{Gte: "20"},
				UpperBound: &api.Range_Lte{Lte: "10"},
			},
			bitSize: 64,
			wantErr: true,
		},
		{
			name: "invalid mixed range (lower > upper)",
			r: &api.Range{
				LowerBound: &api.Range_Gte{Gte: "0"},
				UpperBound: &api.Range_Lte{Lte: "-1"},
			},
			bitSize: 64,
			wantErr: true,
		},
		{
			name: "invalid negative range (lower > upper)",
			r: &api.Range{
				LowerBound: &api.Range_Gte{Gte: "-1"},
				UpperBound: &api.Range_Lte{Lte: "-2"},
			},
			bitSize: 64,
			wantErr: true,
		},
		{
			name: "only negative lower bound",
			r: &api.Range{
				LowerBound: &api.Range_Gte{Gte: "-10"},
			},
			bitSize: 64,
			want: []entities.Range{
				{Operation: "gte", Value: "-10"},
			},
			wantErr: false,
		},
		{
			name: "only negative upper bound",
			r: &api.Range{
				UpperBound: &api.Range_Lte{Lte: "-20"},
			},
			bitSize: 64,
			want: []entities.Range{
				{Operation: "lte", Value: "-20"},
			},
			wantErr: false,
		},
		{
			name:    "no bounds",
			r:       &api.Range{},
			bitSize: 64,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateSignedNumericRange(tt.r, tt.bitSize)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
