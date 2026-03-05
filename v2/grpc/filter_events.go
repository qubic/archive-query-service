package grpc

import (
	"fmt"
	"strconv"

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

func validateEventsFilters(filters map[string][]string) error {
	if len(filters) == 0 {
		return nil
	}

	if len(filters) > len(allowedEventFilters) {
		return fmt.Errorf("too many filters")
	}

	for key, values := range filters {
		switch key {
		case "transactionHash":
			if len(values) != 1 {
				return fmt.Errorf("filter [%s] must have exactly one value", key)
			}
		case "tickNumber":
			if len(values) != 1 {
				return fmt.Errorf("filter [%s] must have exactly one value", key)
			}
			_, err := strconv.ParseUint(values[0], 10, 32)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: must be a valid number but was [%s]", key, values[0])
			}
		case "logType":
			if len(values) != 1 {
				return fmt.Errorf("filter [%s] must have exactly one value", key)
			}

			uVal, err := strconv.ParseUint(values[0], 10, 32)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: must be a valid number but was [%s]", key, values[0])
			}
			if uVal > 14 && uVal != 255 {
				return fmt.Errorf("invalid [%s] filter: must be 0-13 or 255 but was [%d]", key, uVal)
			}

		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}
