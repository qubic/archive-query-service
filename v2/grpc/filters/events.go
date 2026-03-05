package filters

import (
	"fmt"

	"github.com/qubic/archive-query-service/v2/grpc/utils"
)

const (
	EventFilterSource             = "source"
	EventFilterSourceExclude      = "source-exclude"
	EventFilterDestination        = "destination"
	EventFilterDestinationExclude = "destination-exclude"
	EventFilterTransactionHash    = "transactionHash"
	EventFilterTickNumber         = "tickNumber"
	EventFilterLogType            = "logType"
	EventFilterEpoch              = "epoch"
	EventFilterAmount             = "amount"
)

var allowedEventFilters = [3]string{EventFilterTransactionHash, EventFilterTickNumber, EventFilterLogType}

const maxValuesPerEventFilter = 5
const maxValueLengthPerEventFilter = 5*60 + 5 + 4 // 5 IDs + comma + optional spaces

func CreateEventsFilters(filterMap map[string]string) (map[string][]string, error) {

	res := make(map[string][]string)
	for k, v := range filterMap {
		shouldSplit := k == EventFilterSource || k == EventFilterDestination || k == EventFilterSourceExclude || k == EventFilterDestinationExclude

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

func validateEventsFilters(filterMap map[string][]string) error {
	if len(filterMap) == 0 {
		return nil
	}

	if len(filterMap) > len(allowedEventFilters) {
		return fmt.Errorf("too many filters")
	}

	// it's not allowed to use a match-filter and a corresponding exclude-filter at the same time
	if (filterMap[EventFilterSource] != nil && filterMap[EventFilterSourceExclude] != nil) ||
		(filterMap[EventFilterDestination] != nil && filterMap[EventFilterDestinationExclude] != nil) {
		return fmt.Errorf("conflicting filters")
	}

	for key, values := range filterMap {
		switch key {
		case EventFilterSource, EventFilterDestination, EventFilterSourceExclude, EventFilterDestinationExclude:

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

		case EventFilterAmount:

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
