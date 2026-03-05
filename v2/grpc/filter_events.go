package grpc

import (
	"fmt"

	"github.com/qubic/archive-query-service/v2/grpc/filters"
)

var allowedEventFilters = [3]string{"transactionHash", "tickNumber", "logType"}

func createEventsFilters(filterMap map[string]string) (map[string][]string, error) {
	res := make(map[string][]string)
	for k, v := range filterMap {
		vs, err := filters.CreateFilters(v, 1, 60)
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

	for key, values := range filterMap {
		switch key {
		case "transactionHash":

			err := filters.ValidateTransactionHashFilterValues(values, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case "tickNumber":

			err := filters.ValidateUnsignedNumericFilterValues(values, 32, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case "logType":

			err := filters.ValidateUnsignedNumericFilterValues(values, 8, 1) // up to 255
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}
