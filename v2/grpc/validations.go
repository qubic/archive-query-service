package grpc

import (
	"errors"
	"fmt"
	"strconv"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/go-node-connector/types"
)

const maxHitsSize uint32 = 10000

type PageSizeLimits struct {
	maxPageSize     uint32
	defaultPageSize uint32
}

func NewPageSizeLimits(maxPageSize, defaultPageSize uint32) PageSizeLimits {
	return PageSizeLimits{
		maxPageSize:     maxPageSize,
		defaultPageSize: defaultPageSize,
	}
}

func (psl PageSizeLimits) validatePageSize(pageSize, offset uint32) (uint32, error) {
	if pageSize > psl.maxPageSize {
		return 0, fmt.Errorf("page size [%d] exceeds allowed maximum [%d]", pageSize, psl.maxPageSize)
	}

	if pageSize == 0 {
		return psl.defaultPageSize, nil
	}

	if pageSize == 1 {
		if offset != 0 {
			return 0, fmt.Errorf("page size [1] is only allowed for offset [0]")
		}
		return pageSize, nil
	}

	if pageSize%10 != 0 {
		return 0, fmt.Errorf("page size [%d] must be a multiple of [10]", pageSize)
	}

	return pageSize, nil
}

func (psl PageSizeLimits) validatePageOffset(pageSize, offset uint32) (uint32, error) {
	if offset > maxHitsSize {
		return 0, fmt.Errorf("offset [%d] exceeds maximum allowed [%d]", offset, maxHitsSize)
	}

	if offset+pageSize > maxHitsSize {
		return 0, fmt.Errorf("offset [%d] + size [%d] exceeds maximum allowed [%d]", offset, pageSize, maxHitsSize)
	}

	return offset, nil
}

func (psl PageSizeLimits) ValidatePagination(pagination *api.Pagination) (uint32, uint32, error) {
	var pageSize uint32
	var offset uint32

	// Sane defaults if pagination block is missing inside request
	if pagination == nil {
		pageSize = psl.defaultPageSize
		offset = 0
	} else {
		pageSize = pagination.Size
		offset = pagination.Offset
	}

	pageSize, err := psl.validatePageSize(pageSize, offset)
	if err != nil {
		return 0, 0, fmt.Errorf("validating page size: %w", err)
	}

	offset, err = psl.validatePageOffset(pageSize, offset)
	if err != nil {
		return 0, 0, fmt.Errorf("validating page offset: %w", err)
	}

	return offset, pageSize, nil
}

/*func validatePagination(page *api.Pagination) (from uint32, size uint32, err error) {
	from = page.GetOffset()
	if from >= maxHitsSize {
		return 0, 0, fmt.Errorf("offset [%d] exceeds maximum [%d]", from, maxHitsSize)
	}
	size = page.GetSize()
	if size > maxPageSize {
		return 0, 0, fmt.Errorf("size [%d] exceeds maximum [%d]", size, maxPageSize)
	}
	if size == 0 {
		size = defaultPageSize
	}
	if from+size > maxHitsSize {
		return 0, 0, fmt.Errorf("offset [%d] + size [%d] exceeds maximum [%d]", from, size, maxHitsSize)
	}
	return from, size, nil
}*/

var allowedTermFilters = [4]string{"source", "destination", "amount", "inputType"}

func validateIdentityTransactionQueryFilters(filters map[string]string) error {
	if len(filters) == 0 {
		return nil
	}

	if len(filters) > len(allowedTermFilters) {
		return errors.New("too many filters")
	}
	for key, value := range filters {
		switch key {
		case "source":
			err := validateIdentity(value)
			if err != nil {
				return fmt.Errorf("invalid source filter: %w", err)
			}
		case "destination":
			err := validateIdentity(value)
			if err != nil {
				return fmt.Errorf("invalid destination filter: %w", err)
			}
		case "amount":
			_, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid amount filter: %w", err)
			}
		case "inputType":
			_, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid inputType filter: %w", err)
			}
		default:
			return fmt.Errorf("unsupported filter: [%s]", key)
		}
	}
	return nil
}

var allowedRanges = [4]string{"amount", "tickNumber", "inputType", "timestamp"}

func validateIdentityTransactionQueryRanges(filters map[string]string, ranges map[string]*api.Range) (map[string][]*entities.Range, error) {
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
		case "amount":
			r, err := validateRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid amount range: %w", err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case "tickNumber":
			r, err := validateRange(value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid tickNumber range: %w", err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case "inputType":
			r, err := validateRange(value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid inputType range: %w", err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case "timestamp":
			r, err := validateRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp range: %w", err)
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
