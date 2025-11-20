package grpc

import (
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
)

func TestPaginationLimits_ValidatePageSizeLimits(t *testing.T) {

	test := map[string]struct {
		pl       PaginationLimits
		pageSize int
		offset   int

		expectedPageSize int
		expectError      bool
	}{
		"TestEnforceLimitsDisabled": {
			pl:               NewPaginationLimits(false, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         5,
			offset:           0,
			expectedPageSize: 5,
			expectError:      false,
		},
		"TestDefaultPageSize": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         0,
			offset:           0,
			expectedPageSize: 10,
			expectError:      false,
		},
		"TestPageSizeOneForOffsetZero": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         1,
			offset:           0,
			expectedPageSize: 1,
			expectError:      false,
		},
		"TestPageSizeOneForOffsetZero_Negative": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         1,
			offset:           1,
			expectedPageSize: 0,
			expectError:      true,
		},
		"CorrectPageSize": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         25,
			offset:           0,
			expectedPageSize: 25,
			expectError:      false,
		},
		"WrongPageSize": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         13,
			offset:           0,
			expectedPageSize: 0,
			expectError:      true,
		},
	}

	for testName, testData := range test {
		t.Run(testName, func(t *testing.T) {

			_, size, err := testData.pl.ValidatePagination(&api.Pagination{
				Offset: uint32(testData.offset),
				Size:   uint32(testData.pageSize),
			})
			if testData.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, testData.expectedPageSize, size)

		})
	}

}
