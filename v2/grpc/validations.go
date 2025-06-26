package grpc

import (
	"errors"
	"fmt"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/go-node-connector/types"
	"strconv"
)

const maxPageSize uint32 = 1024
const maxHitsSize uint32 = 10000

func validatePage(page *api.Page) error {
	if page.GetSize() > maxPageSize {
		return fmt.Errorf("maximum page size: [%d], got: [%d]", maxPageSize, page.GetSize())
	}
	if page.GetNumber()*page.GetSize() >= maxHitsSize {
		return fmt.Errorf("maximum result size: [%d]", maxHitsSize)
	}
	return nil
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

func validateIdentityTransactionQueryRanges(filters map[string]string, ranges map[string]*api.Range) error {
	if len(ranges) == 0 {
		return nil
	}
	allowedRanges := [4]string{"amount", "tickNumber", "inputType", "timestamp"}
	if len(ranges) > len(allowedRanges) {
		return errors.New("too many ranges")
	}

	for key, value := range ranges {
		switch key {
		case "amount":
			err := validateRange(value, 64)
			if err != nil {
				return fmt.Errorf("invalid amount range: [%w]", err)
			}
		case "tickNumber":
			err := validateRange(value, 32)
			if err != nil {
				return fmt.Errorf("invalid tickNumber range: [%w]", err)
			}
		case "inputType":
			err := validateRange(value, 32)
			if err != nil {
				return fmt.Errorf("invalid inputType range: [%w]", err)
			}
		case "timestamp":
			err := validateRange(value, 64)
			if err != nil {
				return fmt.Errorf("invalid timestamp range: [%w]", err)
			}
		default:
			return fmt.Errorf("unsupported range: [%s]", key)
		}
	}

	if filters != nil {
		// check for ranges that are already declared as filter
		for key := range ranges {
			_, ok := filters[key]
			if ok {
				return fmt.Errorf("range [%s] is already declared as filter", key)
			}
		}
	}
	return nil
}

func validateRange(r *api.Range, bitSize int) error {
	if r != nil {
		gt, err := strconv.ParseUint(r.GetGt(), 10, bitSize)
		if err != nil {
			return fmt.Errorf("invalid [gt] value: %w", err)
		}
		gte, err := strconv.ParseUint(r.GetGte(), 10, bitSize)
		if err != nil {
			return fmt.Errorf("invalid [gte] value: %w", err)
		}
		lt, err := strconv.ParseUint(r.GetLt(), 10, bitSize)
		if err != nil {
			return fmt.Errorf("invalid [lt] value: %w", err)
		}
		lte, err := strconv.ParseUint(r.GetLte(), 10, bitSize)
		if err != nil {
			return fmt.Errorf("invalid [lte] value: %w", err)
		}
		lowerBound := max(gt, gte)
		upperBound := max(lt, lte)
		if lowerBound > 0 && upperBound > 0 && lowerBound >= upperBound {
			return errors.New("upper bound must be larger than lower bound")
		}
	}
	return nil
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
