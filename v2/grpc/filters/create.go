package filters

import (
	"fmt"
	"strconv"
	"strings"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
)

func CreateFilters(value string, maxValues, maxLength int) ([]string, error) {
	// check max length to avoid further more costly processing
	if maxLength > 0 && len(value) > maxLength {
		return nil, fmt.Errorf("exceeds maximum length")
	}

	// count commas first to avoid input with many strings before splitting
	valCount := strings.Count(value, ",")
	if valCount >= maxValues {
		return nil, fmt.Errorf("more than [%d] values", maxValues)
	}

	var err error
	var val []string

	if maxValues > 1 { // split

		val, err = splitFilterValue(value)
		if err != nil {
			return nil, fmt.Errorf("splitting: %w", err)
		}

	} else { // trim only

		val, err = trimFilterValue(value)
		if err != nil {
			return nil, fmt.Errorf("trimming: %w", err)
		}

	}
	return val, nil
}

func CreateNumericRange(r *api.Range, bitSize int) ([]*entities.Range, error) {
	var ranges []*entities.Range
	var err error
	var lowerBound uint64
	var upperBound uint64
	switch r.GetLowerBound().(type) {
	case *api.Range_Gt:
		lowerBound, err = stringToNumericValue(r.GetGt(), bitSize)
		lowerBound++
		if err != nil {
			return nil, fmt.Errorf("invalid [gt] value: %w", err)
		}
		ranges = append(ranges, &entities.Range{
			Operation: "gt",
			Value:     r.GetGt(),
		})
	case *api.Range_Gte:
		lowerBound, err = stringToNumericValue(r.GetGte(), bitSize)
		if err != nil {
			return nil, fmt.Errorf("invalid [gte] value: %w", err)
		}
		ranges = append(ranges, &entities.Range{
			Operation: "gte",
			Value:     r.GetGte(),
		})
	}

	switch r.GetUpperBound().(type) {
	case *api.Range_Lt:
		upperBound, err = stringToNumericValue(r.GetLt(), bitSize)
		upperBound--
		if err != nil {
			return nil, fmt.Errorf("invalid [lt] value: %w", err)
		}
		ranges = append(ranges, &entities.Range{
			Operation: "lt",
			Value:     r.GetLt(),
		})
	case *api.Range_Lte:
		upperBound, err = stringToNumericValue(r.GetLte(), bitSize)
		if err != nil {
			return nil, fmt.Errorf("invalid [lte] value: %w", err)
		}
		ranges = append(ranges, &entities.Range{
			Operation: "lte",
			Value:     r.GetLte(),
		})
	}

	if len(ranges) == 0 {
		return nil, fmt.Errorf("invalid range: no bounds")
	}

	if lowerBound > 0 && upperBound > 0 && lowerBound >= upperBound {
		return nil, fmt.Errorf("invalid range: [%d:%d]", lowerBound, upperBound)
	}

	return ranges, nil
}

func stringToNumericValue(val string, bitSize int) (uint64, error) {
	number, err := strconv.ParseUint(val, 10, bitSize)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func splitFilterValue(value string) ([]string, error) {
	split := strings.Split(value, ",")
	values := make([]string, 0, len(split))
	seen := make(map[string]bool)
	for _, s := range split {
		trimmed := strings.TrimSpace(s)
		if trimmed == "" {
			return nil, fmt.Errorf("contains empty value")
		}
		if seen[trimmed] {
			return nil, fmt.Errorf("contains duplicate value [%s]", trimmed)
		}
		seen[trimmed] = true
		values = append(values, trimmed)
	}
	return values, nil
}

func trimFilterValue(value string) ([]string, error) {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty value")
	}
	return []string{trimmed}, nil
}
