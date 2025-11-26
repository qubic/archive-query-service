package rpc

import (
	"fmt"
	"sort"
)

type PaginationLimits struct {
	enforceLimits         bool
	allowedPageSizes      map[int]bool
	allowedPageSizesArray []int
	defaultPageSize       int
	maxAllowedOffset      int
}

func NewPaginationLimits(enforce bool, pageSizes []int, defaultSize int, maxAllowedOffset int) PaginationLimits {

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
		maxAllowedOffset:      maxAllowedOffset,
	}
}

const maxPageSize = 1024

func (pl PaginationLimits) ValidatePageSizeLimits(pageSize, page int) (int, error) {

	// This check should be run regardless if limits are enabled or not
	if pageSize*page > pl.maxAllowedOffset {
		return 0, fmt.Errorf("pagination out of bounds. pageSize * page cannot be larger than [%d], got: [%d]", pl.maxAllowedOffset, pageSize*page)
	}

	// If disabled use previous behaviour
	if !pl.enforceLimits {

		if pageSize > maxPageSize {
			return 0, fmt.Errorf("size [%d] exceeds maximum [%d]", pageSize, maxPageSize)
		}
		if pageSize == 0 {
			return pl.defaultPageSize, nil
		}

		return pageSize, nil
	}

	if pageSize == 0 {
		return pl.defaultPageSize, nil
	}

	if pageSize == 1 {
		// This check assumes that the first page is 1, not 0
		if page != 1 {
			return 0, fmt.Errorf("page size [1] is only allowed when used with page [1]")
		}
		return pageSize, nil
	}

	if _, exists := pl.allowedPageSizes[pageSize]; !exists {
		return 0, fmt.Errorf("page size [%d] not supported. supported page sizes: %v", pageSize, pl.allowedPageSizesArray)
	}
	return pageSize, nil
}
