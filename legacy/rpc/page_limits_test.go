package rpc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPaginationLimits_ValidatePageSizeLimits(t *testing.T) {

	test := map[string]struct {
		pl       PaginationLimits
		pageSize int
		page     int

		expectedPageSize int
		expectError      bool
	}{
		"TestEnforceLimitsDisabled": {
			pl:               NewPaginationLimits(false, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         5,
			page:             0,
			expectedPageSize: 5,
			expectError:      false,
		},
		"TestDefaultPageSize": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         0,
			page:             0,
			expectedPageSize: 10,
			expectError:      false,
		},
		"TestPageSizeOneForOffsetZero": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         1,
			page:             1,
			expectedPageSize: 1,
			expectError:      false,
		},
		"TestPageSizeZeroForPageOtherThanOne": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         1,
			page:             2,
			expectedPageSize: 0,
			expectError:      true,
		},
		"CorrectPageSize": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         25,
			page:             0,
			expectedPageSize: 25,
			expectError:      false,
		},
		"WrongPageSize": {
			pl:               NewPaginationLimits(true, []int{10, 25, 50, 100}, 10, 10000),
			pageSize:         13,
			page:             0,
			expectedPageSize: 0,
			expectError:      true,
		},
	}

	for testName, testData := range test {
		t.Run(testName, func(t *testing.T) {

			size, err := testData.pl.ValidatePageSizeLimits(testData.pageSize, testData.page)
			if testData.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, testData.expectedPageSize, size)

		})
	}

}
