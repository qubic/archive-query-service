package grpc

import (
	"errors"
	"fmt"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/grpc/filters"
	"github.com/qubic/archive-query-service/v2/grpc/utils"
)

const (
	FilterSource             = "source"
	FilterSourceExclude      = "source-exclude"
	FilterDestination        = "destination"
	FilterDestinationExclude = "destination-exclude"
	FilterAmount             = "amount"
	FilterInputType          = "inputType"
	FilterTickNumber         = "tickNumber"
	FilterTimestamp          = "timestamp"
)

var allowedTermFilters = [7]string{FilterSource, FilterSourceExclude, FilterDestination, FilterDestinationExclude, FilterAmount, FilterInputType, FilterTickNumber}
var allowedTickTermFilters = [4]string{FilterSource, FilterDestination, FilterAmount, FilterInputType}

const maxValuesPerIdentityFilter = 5
const maxValueLengthPerIdentityFilter = 5*60 + 5 + 4 // 5 IDs + comma + optional spaces

func createIdentityTransactionFilters(filterMap map[string]string) (map[string][]string, error) {
	res := make(map[string][]string)
	for k, v := range filterMap {
		shouldSplit := k == FilterSource || k == FilterDestination || k == FilterSourceExclude || k == FilterDestinationExclude

		maxValues := utils.If(shouldSplit, maxValuesPerIdentityFilter, 1)
		maxLength := utils.If(shouldSplit, maxValueLengthPerIdentityFilter, 20)

		vs, err := filters.CreateFilters(v, maxValues, maxLength)
		if err != nil {
			return nil, fmt.Errorf("handling filter [%s]: %w", k, err)
		}
		res[k] = vs

	}

	err := validateIdentityTransactionQueryFilters(res)
	if err != nil {
		return nil, fmt.Errorf("validating filters: %w", err)
	}

	return res, nil
}

func validateIdentityTransactionQueryFilters(filtersMap map[string][]string) error {
	if len(filtersMap) == 0 {
		return nil
	}

	if len(filtersMap) > len(allowedTermFilters) {
		return errors.New("too many filters")
	}

	// it's not allowed to use a match-filter and a corresponding exclude-filter at the same time
	if (filtersMap[FilterSource] != nil && filtersMap[FilterSourceExclude] != nil) ||
		(filtersMap[FilterDestination] != nil && filtersMap[FilterDestinationExclude] != nil) {
		return fmt.Errorf("conflicting filters")
	}

	for key, values := range filtersMap {
		switch key {
		case FilterSource, FilterDestination, FilterSourceExclude, FilterDestinationExclude:
			err := filters.ValidateIdentityFilterValues(values, 5)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		case FilterAmount:
			err := filters.ValidateNumericFilterValues(values, 64, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		case FilterTickNumber, FilterInputType:
			err := filters.ValidateNumericFilterValues(values, 32, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}

var allowedRanges = [4]string{FilterAmount, FilterTickNumber, FilterInputType, FilterTimestamp}
var allowedTickRanges = [2]string{FilterAmount, FilterInputType}

func createIdentityTransactionQueryRanges(filtersMap map[string][]string, ranges map[string]*api.Range) (map[string][]*entities.Range, error) {
	convertedRanges := map[string][]*entities.Range{}
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(ranges) > len(allowedRanges) {
		return nil, errors.New("too many ranges")
	}

	err := filters.VerifyNoFilterDuplicates(filtersMap, ranges)
	if err != nil {
		return nil, fmt.Errorf("checking for duplicate: %w", err)
	}

	for key, value := range ranges {
		switch key {
		case FilterAmount, FilterTimestamp:
			r, err := filters.CreateNumericRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case FilterTickNumber, FilterInputType:
			r, err := filters.CreateNumericRange(value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid %s range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		default:
			return nil, fmt.Errorf("unsupported range: [%s]", key)
		}
	}

	return convertedRanges, nil
}
