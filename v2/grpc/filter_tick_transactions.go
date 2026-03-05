package grpc

import (
	"errors"
	"fmt"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/grpc/filters"
)

func createTickTransactionsFilters(filterMap map[string]string) (map[string][]string, error) {
	res := make(map[string][]string)
	for k, v := range filterMap {
		f, err := filters.CreateFilters(v, 1, 60) // 60 character identity
		if err != nil {
			return nil, fmt.Errorf("creating tick transactions filter [%s]: %w", k, err)
		}
		res[k] = f
	}

	err := validateTickTransactionQueryFilters(res)
	if err != nil {
		return nil, fmt.Errorf("validating filter: %w", err)
	}

	return res, nil
}

func validateTickTransactionQueryFilters(filterMap map[string][]string) error {
	if len(filterMap) == 0 {
		return nil
	}

	if len(filterMap) > len(allowedTickTermFilters) {
		return errors.New("too many filters")
	}

	for key, values := range filterMap {
		switch key {
		case FilterSource, FilterDestination:
			err := filters.ValidateIdentityFilterValues(values, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		case FilterAmount:
			err := filters.ValidateUnsignedNumericFilterValues(values, 64, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		case FilterInputType:
			err := filters.ValidateUnsignedNumericFilterValues(values, 32, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}

func validateTickTransactionQueryRanges(filterMap map[string][]string, ranges map[string]*api.Range) (map[string][]*entities.Range, error) {
	convertedRanges := map[string][]*entities.Range{}
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(ranges) > len(allowedTickRanges) {
		return nil, errors.New("too many ranges")
	}

	err := filters.VerifyNoFilterDuplicates(filterMap, ranges)
	if err != nil {
		return nil, fmt.Errorf("checking for duplicate: %w", err)
	}

	for key, value := range ranges {
		switch key {
		case FilterAmount:
			r, err := filters.CreateNumericRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case FilterInputType:
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
