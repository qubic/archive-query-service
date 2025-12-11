package grpc

import (
	"fmt"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
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

func (psl PageSizeLimits) validatePageSize(pageSize, _ uint32) (uint32, error) {
	if pageSize > psl.maxPageSize {
		return 0, fmt.Errorf("page size [%d] exceeds allowed maximum [%d]", pageSize, psl.maxPageSize)
	}

	if pageSize == 0 {
		return psl.defaultPageSize, nil
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
