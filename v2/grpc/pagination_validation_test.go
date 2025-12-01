package grpc

import (
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageSizeLimits_ValidatePagination_GivenValidPageSize_ThenNoError(t *testing.T) {

	defaultPageLimits := NewPageSizeLimits(1000, 10)

	test := map[string]struct {
		pageSizeLimits   PageSizeLimits
		inputPageSize    uint32
		inputOffset      uint32
		expectedPageSize uint32
		expectedOffset   uint32
	}{
		// valid page sizes
		"TestPageSizeModuloTen_1": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    10,
			inputOffset:      0,
			expectedPageSize: 10,
			expectedOffset:   0,
		},
		"TestPageSizeModuloTen_2": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    100,
			inputOffset:      0,
			expectedPageSize: 100,
			expectedOffset:   0,
		},
		"TestPageSizeMax": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    1000,
			inputOffset:      0,
			expectedPageSize: 1000,
			expectedOffset:   0,
		},
		"TestPageSizeOneForZeroOffset": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    1,
			inputOffset:      0,
			expectedPageSize: 1,
			expectedOffset:   0,
		},
		"TestDefaultPageSize": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    0,
			inputOffset:      0,
			expectedPageSize: 10,
			expectedOffset:   0,
		},
	}

	for testName, testData := range test {

		t.Run(testName, func(t *testing.T) {
			offset, size, err := testData.pageSizeLimits.ValidatePagination(&api.Pagination{
				Offset: testData.inputOffset,
				Size:   testData.inputPageSize,
			})
			require.NoError(t, err)
			assert.Equalf(t, testData.expectedPageSize, size, "PageSize not equal. Expected %d, Got %d", testData.expectedPageSize, size)
			assert.Equalf(t, testData.expectedPageSize, size, "Offset not equal. Expected %d, Got %d", testData.expectedOffset, offset)
		})
	}
}

func TestPageSizeLimits_ValidatePagination_GivenInvalidPageSize_ThenError(t *testing.T) {

	defaultPageLimits := NewPageSizeLimits(1000, 10)

	test := map[string]struct {
		pageSizeLimits   PageSizeLimits
		inputPageSize    uint32
		inputOffset      uint32
		expectedPageSize uint32
		expectedOffset   uint32
	}{
		"TestPageSizeOverMax": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  1010,
			inputOffset:    0,
		},
		"TestPageSizeNotModuloTen_1": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  11,
			inputOffset:    0,
		},
		"TestPageSizeNotModuloTen_2": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  101,
			inputOffset:    0,
		},
		"TestPageSizeNotModuloTen_3": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  999,
			inputOffset:    0,
		},
		"TestPageSizeOneForNonZeroOffset": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  1,
			inputOffset:    1,
		},
	}

	for testName, testData := range test {

		t.Run(testName, func(t *testing.T) {
			_, _, err := testData.pageSizeLimits.ValidatePagination(&api.Pagination{
				Offset: testData.inputOffset,
				Size:   testData.inputPageSize,
			})
			require.Error(t, err)
		})
	}
}

func TestPageSizeLimits_ValidatePagination_GivenValidOffset_ThenNoError(t *testing.T) {

	defaultPageLimits := NewPageSizeLimits(1000, 10)

	test := map[string]struct {
		pageSizeLimits   PageSizeLimits
		inputPageSize    uint32
		inputOffset      uint32
		expectedPageSize uint32
		expectedOffset   uint32
	}{
		// offset tests
		"TestPageSizePlusOffsetUnderMax_1": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    10,
			inputOffset:      9990,
			expectedPageSize: 10,
			expectedOffset:   9990,
		},
		"TestPageSizePlusOffsetUnderMax_2": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    0, // Default page size
			inputOffset:      9990,
			expectedPageSize: 10,
			expectedOffset:   9990,
		},
		"TestPageSizePlusOffsetUnderMax_3": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    100,
			inputOffset:      9900,
			expectedPageSize: 100,
			expectedOffset:   9900,
		},
		"TestValidOffsetForPageSize100": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    100,
			inputOffset:      100,
			expectedPageSize: 100,
			expectedOffset:   100,
		},
		"TestValidOffsetForPageSize10": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    10,
			inputOffset:      90,
			expectedPageSize: 10,
			expectedOffset:   90,
		},
		"TestValidOffsetForPageSize30": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    30,
			inputOffset:      90,
			expectedPageSize: 30,
			expectedOffset:   90,
		},
	}

	for testName, testData := range test {

		t.Run(testName, func(t *testing.T) {
			offset, size, err := testData.pageSizeLimits.ValidatePagination(&api.Pagination{
				Offset: testData.inputOffset,
				Size:   testData.inputPageSize,
			})
			require.NoError(t, err)
			assert.Equalf(t, testData.expectedPageSize, size, "PageSize not equal. Expected %d, Got %d", testData.expectedPageSize, size)
			assert.Equalf(t, testData.expectedPageSize, size, "Offset not equal. Expected %d, Got %d", testData.expectedOffset, offset)
		})
	}
}

func TestPageSizeLimits_ValidatePagination_GivenInvalidOffset_ThenError(t *testing.T) {

	defaultPageLimits := NewPageSizeLimits(1000, 10)

	test := map[string]struct {
		pageSizeLimits PageSizeLimits
		inputPageSize  uint32
		inputOffset    uint32
		errorMsg       string
	}{
		"TestOffsetIsMax": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  10,
			inputOffset:    10000,
			errorMsg:       "exceeds maximum allowed",
		},
		"TestDefaultPageSizeOverMax": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  0, // Default page size
			inputOffset:    10000,
			errorMsg:       "exceeds maximum allowed",
		},
		"TestPageSize100PlusOffsetOverMax": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  100,
			inputOffset:    9990,
			errorMsg:       "exceeds maximum allowed",
		},
		"TestInvalidOffsetForPageSize1000": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  1000,
			inputOffset:    100,
			errorMsg:       "multiple of the page size [1000]",
		},
		"TestInvalidOffsetForPageSize100": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  100,
			inputOffset:    10,
			errorMsg:       "multiple of the page size [100]",
		},
		"TestInvalidOffsetForPageSize10": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  10,
			inputOffset:    95,
			errorMsg:       "multiple of the page size [10]",
		},
		"TestInvalidOffsetForPageSize30": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  30,
			inputOffset:    100,
			errorMsg:       "multiple of the page size [30]",
		},
		"TestInvalidOffsetForDefaultPageSize": {
			pageSizeLimits: defaultPageLimits,
			inputPageSize:  0, // defaults to 10
			inputOffset:    5,
			errorMsg:       "multiple of the page size [10]",
		},
	}

	for testName, testData := range test {

		t.Run(testName, func(t *testing.T) {
			_, _, err := testData.pageSizeLimits.ValidatePagination(&api.Pagination{
				Offset: testData.inputOffset,
				Size:   testData.inputPageSize,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), testData.errorMsg)
		})
	}
}
