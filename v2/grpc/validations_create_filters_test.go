package grpc

import (
	"reflect"
	"testing"
)

func Test_createFilters(t *testing.T) {
	tests := []struct {
		name    string
		filters map[string]string
		want    map[string][]string
		wantErr bool
	}{
		{
			name: "single value for source",
			filters: map[string]string{
				"source": "value1",
			},
			want: map[string][]string{
				"source": {"value1"},
			},
			wantErr: false,
		},
		{
			name: "multiple values for source",
			filters: map[string]string{
				"source": "value1,value2,value3",
			},
			want: map[string][]string{
				"source": {"value1", "value2", "value3"},
			},
			wantErr: false,
		},
		{
			name: "multiple values with spaces for destination",
			filters: map[string]string{
				"destination": " value1 , value2,  value3 ",
			},
			want: map[string][]string{
				"destination": {"value1", "value2", "value3"},
			},
			wantErr: false,
		},
		{
			name: "no splitting for other keys",
			filters: map[string]string{
				"other": "value1,value2,value3",
			},
			want: map[string][]string{
				"other": {"value1,value2,value3"},
			},
			wantErr: false,
		},
		{
			name: "no splitting for other keys with spaces",
			filters: map[string]string{
				"another": "  value1, value2  ",
			},
			want: map[string][]string{
				"another": {"value1, value2"},
			},
			wantErr: false,
		},
		{
			name: "empty values for source-exclude",
			filters: map[string]string{
				"source-exclude": "value1,,value2",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "too many values for destination-exclude",
			filters: map[string]string{
				"destination-exclude": "1,2,3,4,5,6",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "duplicate values for source",
			filters: map[string]string{
				"source": "value1,value2,value1",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "empty source filter",
			filters: map[string]string{
				"source": "",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "empty value for other key",
			filters: map[string]string{
				"other": "  ",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createFilters(tt.filters)
			if (err != nil) != tt.wantErr {
				t.Errorf("createFilters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createFilters() got = %v, want %v", got, tt.want)
			}
		})
	}
}
