package grpc

import (
	"testing"

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
			pl:               NewPaginationLimits(false, []int{10, 25, 50, 100}, 10),
			pageSize:         5,
			offset:           0,
			expectedPageSize: 5,
			expectError:      false,
		},
		"TestDefaultPageSize": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10),
			pageSize:         0,
			offset:           0,
			expectedPageSize: 10,
			expectError:      false,
		},
		"TestPageSizeOneForOffsetZero": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10),
			pageSize:         1,
			offset:           0,
			expectedPageSize: 1,
			expectError:      false,
		},
		"TestPageSizeOneForOffsetZero_Negative": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10),
			pageSize:         1,
			offset:           1,
			expectedPageSize: 0,
			expectError:      true,
		},
		"CorrectPageSize": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10),
			pageSize:         25,
			offset:           0,
			expectedPageSize: 25,
			expectError:      false,
		},
		"WrongPageSize": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10),
			pageSize:         13,
			offset:           0,
			expectedPageSize: 0,
			expectError:      true,
		},
	}

	for testName, testData := range test {
		t.Run(testName, func(t *testing.T) {

			pageSize, err := testData.pl.ValidatePageSizeLimits(testData.pageSize, testData.offset)
			if testData.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, testData.expectedPageSize, pageSize)

		})
	}

}
