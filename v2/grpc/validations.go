package grpc

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/go-node-connector/types"
)

const (
	FilterSource             = "source"
	FilterSourceExclude      = "source-exclude"
	FilterDestination        = "destination"
	FilterDestinationExclude = "destination-exclude"
	FilterAmount             = "amount"
	FilterInputType          = "inputType"
	FilterTickNumber         = "tickNumber"
	FilterTimestamp          = "timestamp"
)

var allowedTermFilters = [6]string{FilterSource, FilterSourceExclude, FilterDestination, FilterDestinationExclude, FilterAmount, FilterInputType}

const maxValuesPerFilter = 5

func createFilters(filters map[string]string) (map[string][]string, error) {
	res := make(map[string][]string)
	for k, v := range filters {
		shouldSplit := k == FilterSource || k == FilterDestination || k == FilterSourceExclude || k == FilterDestinationExclude

		if !shouldSplit {
			trimmed := strings.TrimSpace(v)
			if trimmed == "" {
				return nil, fmt.Errorf("filter %s contains an empty value", k)
			}
			res[k] = []string{trimmed}
			continue
		}

		// count commas first to avoid input with many strings before splitting
		valCount := strings.Count(v, ",")
		if valCount >= maxValuesPerFilter {
			return nil, fmt.Errorf("filter %s has more than 5 values", k)
		}

		split := strings.Split(v, ",")
		values := make([]string, 0, len(split))
		seen := make(map[string]bool)
		for _, s := range split {
			trimmed := strings.TrimSpace(s)
			if trimmed == "" {
				return nil, fmt.Errorf("filter %s contains an empty value", k)
			}
			if seen[trimmed] {
				return nil, fmt.Errorf("filter %s contains duplicate value: %s", k, trimmed)
			}
			seen[trimmed] = true
			values = append(values, trimmed)
		}

		res[k] = values
	}
	return res, nil
}

func validateIdentityTransactionQueryFilters(filters map[string][]string) error {
	if len(filters) == 0 {
		return nil
	}

	if len(filters) > len(allowedTermFilters) {
		return errors.New("too many filters")
	}

	// it's not allowed to use a match-filter and a corresponding exclude-filter at the same time
	if (filters[FilterSource] != nil && filters[FilterSourceExclude] != nil) ||
		(filters[FilterDestination] != nil && filters[FilterDestinationExclude] != nil) {
		return fmt.Errorf("conflicting filters")
	}

	for key, values := range filters {
		switch key {
		case FilterSource, FilterDestination, FilterSourceExclude, FilterDestinationExclude:
			for _, val := range values {
				err := validateIdentity(val)
				if err != nil {
					return fmt.Errorf("invalid %s filter: %w", key, err)
				}
			}
		case FilterAmount:
			for _, val := range values {
				_, err := strconv.ParseUint(val, 10, 64)
				if err != nil {
					return fmt.Errorf("invalid %s filter: %w", key, err)
				}
			}
		case FilterInputType:
			for _, val := range values {
				_, err := strconv.ParseUint(val, 10, 32)
				if err != nil {
					return fmt.Errorf("invalid %s filter: %w", key, err)
				}
			}
		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}

var allowedRanges = [4]string{FilterAmount, FilterTickNumber, FilterInputType, FilterTimestamp}

func validateIdentityTransactionQueryRanges(filters map[string][]string, ranges map[string]*api.Range) (map[string][]*entities.Range, error) {
	convertedRanges := map[string][]*entities.Range{}
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(ranges) > len(allowedRanges) {
		return nil, errors.New("too many ranges")
	}

	if filters != nil {
		// check for ranges that are already declared as filter
		for key := range ranges {
			_, ok := filters[key]
			if ok {
				return nil, fmt.Errorf("range [%s] is already declared as filter", key)
			}
		}
	}

	for key, value := range ranges {
		switch key {
		case FilterAmount, FilterTimestamp:
			r, err := validateRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case FilterTickNumber, FilterInputType:
			r, err := validateRange(value, 32)
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

func validateIdentity(identity string) error {
	return validateDigest(identity, false)
}

func validateDigest(digest string, isLowerCase bool) error {
	id := types.Identity(digest)
	pubKey, err := id.ToPubKey(isLowerCase)
	if err != nil {
		return fmt.Errorf("converting id to pubkey: %w", err)
	}

	var pubkeyFixed [32]byte
	copy(pubkeyFixed[:], pubKey[:32])
	id, err = id.FromPubKey(pubkeyFixed, isLowerCase)
	if err != nil {
		return fmt.Errorf("converting pubkey back to id: %w", err)
	}

	if id.String() != digest {
		return fmt.Errorf("invalid %s [%s]", If(isLowerCase, "hash", "identity"), digest)
	}
	return nil
}

func validateRange(r *api.Range, bitSize int) ([]*entities.Range, error) {
	var ranges []*entities.Range
	var err error
	var lowerBound uint64
	var upperBound uint64
	switch r.GetLowerBound().(type) {
	case *api.Range_Gt:
		lowerBound, err = strconv.ParseUint(r.GetGt(), 10, bitSize)
		lowerBound++
		if err != nil {
			return nil, fmt.Errorf("invalid [gt] value: %w", err)
		}
		ranges = append(ranges, &entities.Range{
			Operation: "gt",
			Value:     r.GetGt(),
		})
	case *api.Range_Gte:
		lowerBound, err = strconv.ParseUint(r.GetGte(), 10, bitSize)
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
		upperBound, err = strconv.ParseUint(r.GetLt(), 10, bitSize)
		upperBound--
		if err != nil {
			return nil, fmt.Errorf("invalid [lt] value: %w", err)
		}
		ranges = append(ranges, &entities.Range{
			Operation: "lt",
			Value:     r.GetLt(),
		})
	case *api.Range_Lte:
		upperBound, err = strconv.ParseUint(r.GetLte(), 10, bitSize)
		if err != nil {
			return nil, fmt.Errorf("invalid [lte] value: %w", err)
		}
		ranges = append(ranges, &entities.Range{
			Operation: "lte",
			Value:     r.GetLte(),
		})
	}

	if len(ranges) == 0 {
		return nil, errors.New("invalid range: no bounds")
	}

	if lowerBound > 0 && upperBound > 0 && lowerBound >= upperBound {
		return nil, fmt.Errorf("invalid range: [%d:%d]", lowerBound, upperBound)
	}

	return ranges, nil
}

func If[T any](condition bool, trueValue, falseValue T) T {
	if condition {
		return trueValue
	}

	return falseValue
}
