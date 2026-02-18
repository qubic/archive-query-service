package grpc

import (
	"fmt"
	"strconv"
	"strings"
)

var allowedEventTypes = map[string]bool{
	"0": true, "1": true, "2": true, "3": true, "8": true, "13": true,
}

var allowedEventFilters = [3]string{"transactionHash", "tickNumber", "eventType"}

func createEventsFilters(filters map[string]string) (map[string][]string, error) {
	res := make(map[string][]string)
	for k, v := range filters {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil, fmt.Errorf("filter %s contains an empty value", k)
		}
		res[k] = []string{trimmed}
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
				return fmt.Errorf("filter %s must have exactly one value", key)
			}
		case "tickNumber":
			if len(values) != 1 {
				return fmt.Errorf("filter %s must have exactly one value", key)
			}
			_, err := strconv.ParseUint(values[0], 10, 32)
			if err != nil {
				return fmt.Errorf("invalid %s filter: must be a valid number", key)
			}
		case "eventType":
			if len(values) != 1 {
				return fmt.Errorf("filter %s must have exactly one value", key)
			}
			if !allowedEventTypes[values[0]] {
				return fmt.Errorf("invalid eventType filter: must be one of 0, 1, 2, 3, 8, 13")
			}
		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}
