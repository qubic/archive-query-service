package grpc

import (
	"errors"
	"fmt"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/go-node-connector/types"
	"strconv"
)

const maxPageSize uint32 = 1024
const maxHitsSize uint32 = 10000
const defaultPageSize = 10

func validatePagination(page *api.Pagination) (from uint32, size uint32, err error) {
	from = page.GetOffset()
	if from >= maxHitsSize {
		return 0, 0, fmt.Errorf("offset [%d] exceeds maximum [%d]", from, maxHitsSize)
	}
	size = page.GetSize()
	if size > maxPageSize {
		return 0, 0, fmt.Errorf("size [%d] exceeds maximum [%d]", size, maxPageSize)
	}
	return from, If(size > 0, size, defaultPageSize), nil
}

func validateIdentityTransactionQueryFilters(filters map[string]string) error {
	if len(filters) == 0 {
		return nil
	}
	allowedFilters := [6]string{"source", "destination", "amount", "tickNumber", "inputType", "timestamp"}
	if len(filters) > len(allowedFilters) {
		return errors.New("too many filters")
	}
	for key, value := range filters {
		switch key {
		case "source":
			err := validateIdentity(value)
			if err != nil {
				return fmt.Errorf("invalid source filter: [%w]", err)
			}
		case "destination":
			err := validateIdentity(value)
			if err != nil {
				return fmt.Errorf("invalid destination filter: [%w]", err)
			}
		case "amount":
			_, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid amount filter: [%w]", err)
			}
		case "tickNumber":
			// max allowed tick number is already validated in middleware
			_, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid tickNumber filter: [%w]", err)
			}
		case "inputType":
			_, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid inputType filter: [%w]", err)
			}
		case "timestamp":
			_, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid timestamp filter: [%w]", err)
			}
		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}

func validateIdentityTransactionQueryRanges(filters map[string]string, ranges map[string]*api.Range) (map[string][]*entities.Range, error) {
	convertedRanges := map[string][]*entities.Range{}
	if len(ranges) == 0 {
		return nil, nil
	}
	allowedRanges := [4]string{"amount", "tickNumber", "inputType", "timestamp"}
	if len(ranges) > len(allowedRanges) {
		return nil, errors.New("too many ranges")
	}

	for key, value := range ranges {
		switch key {
		case "amount":
			r, err := validateRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid amount range: [%w]", err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case "tickNumber":
			r, err := validateRange(value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid tickNumber range: [%w]", err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case "inputType":
			r, err := validateRange(value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid inputType range: [%w]", err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case "timestamp":
			r, err := validateRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp range: [%w]", err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		default:
			return nil, fmt.Errorf("unsupported range: [%s]", key)
		}
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
		return fmt.Errorf("original id string %s does not match expected %s", digest, id.String())
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

	if lowerBound > 0 && upperBound > 0 && lowerBound >= upperBound {
		return nil, errors.New("upper bound must be larger than lower bound")
	}
	return ranges, nil
}

func If[T any](condition bool, trueValue, falseValue T) T {
	if condition {
		return trueValue
	} else {
		return falseValue
	}
}
