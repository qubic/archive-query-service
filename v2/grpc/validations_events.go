package grpc

import (
	"fmt"
	"strconv"
	"strings"
)

var allowedEventFilters = [3]string{"transactionHash", "tickNumber", "logType"}

const maxLogType = 13
const maxLogTypeValues = maxLogType + 1 + 1 // 0-maxLogType + type 255

func createEventsFilters(filters map[string]string) (map[string][]string, error) {
	res := make(map[string][]string)
	for k, v := range filters {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil, fmt.Errorf("filter %s contains an empty value", k)
		}
		if k == "logType" && strings.Contains(trimmed, ",") {
			split := strings.Split(trimmed, ",")
			if len(split) > maxLogTypeValues {
				return nil, fmt.Errorf("filter %s has more than %d values", k, maxLogTypeValues)
			}
			values := make([]string, 0, len(split))
			for _, s := range split {
				val := strings.TrimSpace(s)
				if val == "" {
					return nil, fmt.Errorf("filter %s contains an empty value", k)
				}
				values = append(values, val)
			}
			res[k] = values
		} else {
			res[k] = []string{trimmed}
		}
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
			for _, v := range values {
				uVal, err := strconv.ParseUint(v, 10, 32)
				if err != nil {
					return fmt.Errorf("invalid [%s] filter: must be a valid number but was [%s]", key, v)
				}
				if uVal > maxLogType && uVal != 255 {
					return fmt.Errorf("invalid [%s] filter: must be 0-%d or 255 but was [%d]", key, maxLogType, uVal)
				}
			}
		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}
