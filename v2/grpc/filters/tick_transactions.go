package filters

import (
	"errors"
	"fmt"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
)

const (
	TickFilterSource      = "source"
	TickFilterDestination = "destination"
	TickFilterAmount      = "amount"
	TickFilterInputType   = "inputType"
	TickFilterTickNumber  = "tickNumber"
	TickFilterTimestamp   = "timestamp"
)

var allowedTickTermFilters = [4]string{TickFilterSource, TickFilterDestination, TickFilterAmount, TickFilterInputType}

func CreateTickTransactionsFilters(filterMap map[string]string) (map[string][]string, error) {
	res := make(map[string][]string)
	for k, v := range filterMap {
		f, err := CreateFilters(v, 1, 60) // 60 character identity
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
		case TickFilterSource, TickFilterDestination:
			err := ValidateIdentityFilterValues(values, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		case TickFilterAmount:
			err := ValidateUnsignedNumericFilterValues(values, 64, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}
		case TickFilterInputType:
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

const allowedNumberOfTickQueryRanges = 2

func ValidateTickTransactionQueryRanges(filterMap map[string][]string, ranges map[string]*api.Range) (map[string][]*entities.Range, error) {
	convertedRanges := map[string][]*entities.Range{}
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(ranges) > allowedNumberOfTickQueryRanges {
		return nil, fmt.Errorf("too many ranges (%d)", len(ranges))
	}

	err := VerifyNoFilterDuplicates(filterMap, ranges)
	if err != nil {
		return nil, fmt.Errorf("checking for duplicate: %w", err)
	}

	for key, value := range ranges {
		switch key {
		case TickFilterAmount:
			r, err := CreateNumericRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid [%s] range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case TickFilterInputType:
			r, err := CreateNumericRange(value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid [%s] range: %w", key, err)
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
