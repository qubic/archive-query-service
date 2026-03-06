package filters

import (
	"fmt"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/grpc/utils"
)

const (
	IdentityFilterSource      = "source"
	IdentityFilterDestination = "destination"
	IdentityFilterAmount      = "amount"
	IdentityFilterInputType   = "inputType"
	IdentityFilterTickNumber  = "tickNumber"
	IdentityFilterTimestamp   = "timestamp"
)

const maxValuesPerIdentityFilter = 5
const maxValueLengthPerIdentityFilter = 5*60 + 5 + 4 // 5 IDs + comma + optional spaces
const maxNumberOfPerIdentityFilters = 5

func CreateIdentityTransactionFilters(filterMap map[string]string) (map[string][]string, error) {
	res := make(map[string][]string)
	for k, v := range filterMap {
		shouldSplit := k == IdentityFilterSource || k == IdentityFilterDestination

		maxValues := utils.If(shouldSplit, maxValuesPerIdentityFilter, 1)
		maxLength := utils.If(shouldSplit, maxValueLengthPerIdentityFilter, 20)

		vs, err := CreateFilters(v, maxValues, maxLength)
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

func ValidateExcludeFilterKeys(excludeFilters map[string][]string) error {
	for k := range excludeFilters {
		if k != EventFilterSource && k != EventFilterDestination {
			return fmt.Errorf("unsupported exclude filter [%s]", k)
		}
	}
	return nil
}

func validateIdentityTransactionQueryFilters(filterMap map[string][]string) error {
	if len(filterMap) == 0 {
		return nil
	}

	if len(filterMap) > maxNumberOfPerIdentityFilters {
		return fmt.Errorf("too many filters (%d)", len(filterMap))
	}

	for key, values := range filterMap {
		switch key {
		case IdentityFilterSource, IdentityFilterDestination:
			err := ValidateIdentityFilterValues(values, maxValuesPerIdentityFilter)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		case IdentityFilterAmount:
			err := ValidateUnsignedNumericFilterValues(values, 64, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		case IdentityFilterTickNumber, IdentityFilterInputType:
			err := ValidateUnsignedNumericFilterValues(values, 32, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}

const allowedNumberOfPerIdentityQueryRanges = 4

func CreateIdentityTransactionQueryRanges(includeFilters, excludeFilters map[string][]string, ranges map[string]*api.Range) (map[string][]*entities.Range, error) {
	convertedRanges := map[string][]*entities.Range{}
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(ranges) > allowedNumberOfPerIdentityQueryRanges {
		return nil, fmt.Errorf("too many ranges (%d)", len(ranges))
	}

	err := VerifyNoFilterDuplicates(includeFilters, ranges)
	if err != nil {
		return nil, fmt.Errorf("checking for duplicate: %w", err)
	}

	err = VerifyNoFilterDuplicates(excludeFilters, ranges)
	if err != nil {
		return nil, fmt.Errorf("checking for duplicate: %w", err)
	}

	for key, value := range ranges {
		switch key {
		case IdentityFilterAmount, IdentityFilterTimestamp:
			r, err := CreateNumericRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case IdentityFilterTickNumber, IdentityFilterInputType:
			r, err := CreateNumericRange(value, 32)
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
