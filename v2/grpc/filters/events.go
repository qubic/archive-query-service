package filters

import (
	"fmt"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/grpc/utils"
)

const (
	EventFilterSource          = "source"
	EventFilterDestination     = "destination"
	EventFilterTransactionHash = "transactionHash"
	EventFilterTickNumber      = "tickNumber"
	EventFilterLogType         = "logType"
	EventFilterEpoch           = "epoch"
	EventFilterAmount          = "amount"
	EventFilterNumberOfShares  = "numberOfShares"
	EventRangeTimestamp        = "timestamp"
)

const maxFilters = 10

const maxValuesPerEventFilter = 5
const maxValueLengthPerEventFilter = 5*60 + 5 + 4 // 5 IDs + comma + optional spaces

func CreateEventsFilters(filterMap map[string]string) (map[string][]string, error) {

	res := make(map[string][]string)
	for k, v := range filterMap {
		shouldSplit := k == EventFilterSource || k == EventFilterDestination

		maxValues := utils.If(shouldSplit, maxValuesPerEventFilter, 1)
		maxLength := utils.If(shouldSplit, maxValueLengthPerEventFilter, 60)

		vs, err := CreateFilters(v, maxValues, maxLength)
		if err != nil {
			return nil, fmt.Errorf("handling filter [%s]: %w", k, err)
		}
		res[k] = vs

	}

	err := validateEventsFilters(res)
	if err != nil {
		return nil, fmt.Errorf("validating filter: %w", err)
	}

	return res, nil
}

func VerifyExcludeFilterKeys(excludeFilters map[string][]string) error {
	for k := range excludeFilters {
		if k != EventFilterSource && k != EventFilterDestination {
			return fmt.Errorf("invalid exclude filter [%s]", k)
		}
	}
	return nil
}

func CheckForConflictingFilters(includeFilters, excludeFilters map[string][]string) error {
	for k := range excludeFilters {
		if _, found := includeFilters[k]; found {
			return fmt.Errorf("include and exclude [%s] filter", k)
		}
	}
	return nil
}

const maxNumberOfEventQueryRanges = 5

func CreateEventQueryRanges(includeFilters, excludeFilters map[string][]string, ranges map[string]*api.Range) (map[string][]*entities.Range, error) {
	convertedRanges := map[string][]*entities.Range{}
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(ranges) > maxNumberOfEventQueryRanges {
		return nil, fmt.Errorf("too many ranges (%d)", len(ranges))
	}

	err := VerifyNoFilterDuplicates(includeFilters, ranges)
	if err != nil {
		return nil, fmt.Errorf("checking for duplicate filters: %w", err)
	}

	err = VerifyNoFilterDuplicates(excludeFilters, ranges)
	if err != nil {
		return nil, fmt.Errorf("checking for duplicate filters: %w", err)
	}

	for key, value := range ranges {
		switch key {
		case EventFilterAmount, EventFilterNumberOfShares, EventRangeTimestamp:
			r, err := CreateNumericRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid [%s] range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case EventFilterTickNumber:
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

func validateEventsFilters(filterMap map[string][]string) error {
	if len(filterMap) == 0 {
		return nil
	}

	if len(filterMap) > maxFilters {
		return fmt.Errorf("too many filters (%d)", len(filterMap))
	}

	for key, values := range filterMap {
		switch key {
		case EventFilterSource, EventFilterDestination:

			err := ValidateIdentityFilterValues(values, maxValuesPerEventFilter)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case EventFilterTransactionHash:

			err := ValidateTransactionHashFilterValues(values, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case EventFilterTickNumber, EventFilterEpoch:

			err := ValidateUnsignedNumericFilterValues(values, 32, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case EventFilterAmount, EventFilterNumberOfShares:

			err := ValidateUnsignedNumericFilterValues(values, 64, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case EventFilterLogType:

			err := ValidateUnsignedNumericFilterValues(values, 8, 1) // up to 255
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}
