package grpc

import (
	"fmt"
	"sort"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
)

type PaginationLimits struct {
	enforceLimits         bool
	allowedPageSizes      map[int]bool
	allowedPageSizesArray []int
	defaultPageSize       int
	maxHitsSize           int
}

func NewPaginationLimits(enforce bool, pageSizes []int, defaultSize int, maxHitsSize int) PaginationLimits {

	allowedPageSizes := make(map[int]bool, len(pageSizes))
	for _, size := range pageSizes {
		allowedPageSizes[size] = true
	}

	sorted := append([]int{}, pageSizes...)
	sort.Ints(sorted)

	return PaginationLimits{
		enforceLimits:         enforce,
		allowedPageSizes:      allowedPageSizes,
		allowedPageSizesArray: sorted,
		defaultPageSize:       defaultSize,
		maxHitsSize:           maxHitsSize,
	}
}

func (pl PaginationLimits) ValidatePagination(page *api.Pagination) (from, size int, err error) {

	size, err = pl.validatePageSizeLimits(int(page.Size), int(page.Offset))
	if err != nil {
		return 0, 0, fmt.Errorf("validating page size limits: %w", err)
	}

	from, err = pl.validatePageOffsetLimits(size, int(page.Offset))
	if err != nil {
		return 0, 0, fmt.Errorf("validating page offset limits: %w", err)
	}
	return from, size, nil
}

func (pl PaginationLimits) validatePageSizeLimits(pageSize, offset int) (int, error) {
	if !pl.enforceLimits {
		return pageSize, nil
	}

	if pageSize == 0 {
		return pl.defaultPageSize, nil
	}

	if pageSize == 1 {
		if offset != 0 {
			return 0, fmt.Errorf("page size [1] is only allowed when used with offset [0]")
		}
		return pageSize, nil
	}

	if _, exists := pl.allowedPageSizes[pageSize]; !exists {
		return 0, fmt.Errorf("page size [%d] not supported. supported page sizes: %v", pageSize, pl.allowedPageSizesArray)
	}
	return pageSize, nil
}

func (pl PaginationLimits) validatePageOffsetLimits(pageSize, offset int) (int, error) {

	if offset >= pl.maxHitsSize {
		return 0, fmt.Errorf("offset [%d] exceeds maximum [%d]", offset, pl.maxHitsSize)
	}
	if offset+pageSize > pl.maxHitsSize {
		return 0, fmt.Errorf("offset [%d] + size [%d] exceeds maximum [%d]", offset, pageSize, pl.maxHitsSize)
	}

	return offset, nil
}
