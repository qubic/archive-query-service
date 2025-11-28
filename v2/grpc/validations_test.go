package grpc

import (
	"testing"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/stretchr/testify/require"
)

func TestPageSizeLimits_ValidatePagination(t *testing.T) {

	defaultPageLimits := NewPageSizeLimits(1000, 10)

	test := map[string]struct {
		pageSizeLimits PageSizeLimits

		inputPageSize uint32
		inputOffset   uint32

		expectedPageSize uint32
		expectedOffset   uint32
		expectError      bool
	}{
		"TestPageSizeModuloTen_1": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    10,
			inputOffset:      0,
			expectedPageSize: 10,
			expectedOffset:   0,
			expectError:      false,
		},
		"TestPageSizeModuloTen_2": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    100,
			inputOffset:      0,
			expectedPageSize: 100,
			expectedOffset:   0,
			expectError:      false,
		},
		"TestPageSizeModuloTen_3": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    1000,
			inputOffset:      0,
			expectedPageSize: 1000,
			expectedOffset:   0,
			expectError:      false,
		},
		"TestPageSizeNotModuloTen_1": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    11,
			inputOffset:      0,
			expectedPageSize: 0,
			expectedOffset:   0,
			expectError:      true,
		},
		"TestPageSizeNotModuloTen_2": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    101,
			inputOffset:      0,
			expectedPageSize: 0,
			expectedOffset:   0,
			expectError:      true,
		},
		"TestPageSizeNotModuloTen_3": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    999,
			inputOffset:      0,
			expectedPageSize: 0,
			expectedOffset:   0,
			expectError:      true,
		},
		"TestPageSizeUnderMax": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    1000,
			inputOffset:      0,
			expectedPageSize: 1000,
			expectedOffset:   0,
			expectError:      false,
		},
		"TestPageSizeOverMax": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    1010,
			inputOffset:      0,
			expectedPageSize: 0,
			expectedOffset:   0,
			expectError:      true,
		},
		"TestPageSizeOneForZeroOffset": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    1,
			inputOffset:      0,
			expectedPageSize: 1,
			expectedOffset:   0,
			expectError:      false,
		},
		"TestPageSizeOneForNonZeroOffset": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    1,
			inputOffset:      1,
			expectedPageSize: 0,
			expectedOffset:   0,
			expectError:      true,
		},
		"TestDefaultPageSize": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    0,
			inputOffset:      0,
			expectedPageSize: 10,
			expectedOffset:   0,
			expectError:      false,
		},
		"TestOffsetOverMax": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    10,
			inputOffset:      10000,
			expectedPageSize: 0,
			expectedOffset:   0,
			expectError:      true,
		},
		"TestPageSizePlusOffsetUnderMax_1": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    10,
			inputOffset:      9990,
			expectedPageSize: 10,
			expectedOffset:   9990,
			expectError:      false,
		},
		"TestPageSizePlusOffsetUnderMax_2": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    0, // Default page size
			inputOffset:      9990,
			expectedPageSize: 10,
			expectedOffset:   9990,
			expectError:      false,
		},
		"TestPageSizePlusOffsetUnderMax_3": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    100,
			inputOffset:      9900,
			expectedPageSize: 100,
			expectedOffset:   9900,
			expectError:      false,
		},
		"TestPageSizePlusOffsetOverMax_1": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    10,
			inputOffset:      9991,
			expectedPageSize: 0,
			expectedOffset:   0,
			expectError:      true,
		},
		"TestPageSizePlusOffsetOverMax_2": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    0, // Default page size
			inputOffset:      9991,
			expectedPageSize: 0,
			expectedOffset:   0,
			expectError:      true,
		},
		"TestPageSizePlusOffsetOverMax_3": {
			pageSizeLimits:   defaultPageLimits,
			inputPageSize:    100,
			inputOffset:      9901,
			expectedPageSize: 0,
			expectedOffset:   0,
			expectError:      true,
		},
	}

	for testName, testData := range test {

		t.Run(testName, func(t *testing.T) {
			offset, size, err := testData.pageSizeLimits.ValidatePagination(&api.Pagination{
				Offset: testData.inputOffset,
				Size:   testData.inputPageSize,
			})
			if testData.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				require.Equalf(t, testData.expectedPageSize, size, "PageSize not equal. Expected %d, Got %d", testData.expectedPageSize, size)
				require.Equalf(t, testData.expectedPageSize, size, "Offset not equal. Expected %d, Got %d", testData.expectedOffset, offset)
			}
		})
	}
}

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
