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
}

func NewPaginationLimits(enforce bool, pageSizes []int, defaultSize int) PaginationLimits {

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
	}
}

func (pl PaginationLimits) ValidatePageSizeLimits(pageSize, offset int) (int, error) {
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
