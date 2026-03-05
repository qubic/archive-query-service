package filters

import (
	"fmt"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/grpc/utils"
)

func VerifyNoFilterDuplicates(filterMap map[string][]string, ranges map[string]*api.Range) error {
	if filterMap != nil {
		// check for ranges that are already declared as filter
		for key := range ranges {
			_, ok := filterMap[key]
			if ok {
				return fmt.Errorf("[%s] is already declared", key)
			}
		}
	}
	return nil
}

func ValidateNumericFilterValues(values []string, bitSize, maxValues int) error {
	if len(values) == 0 || len(values) > maxValues {
		return fmt.Errorf("invalid number of values: [%d]", maxValues)
	}
	for _, val := range values {
		_, err := stringToNumericValue(val, bitSize)
		if err != nil {
			return fmt.Errorf("invalid numeric value: %w", err)
		}
	}
	return nil
}

func ValidateIdentityFilterValues(values []string, maxValues int) error {
	if len(values) == 0 || len(values) > maxValues {
		return fmt.Errorf("invalid number of values: [%d]", maxValues)
	}
	for _, val := range values {
		err := utils.ValidateIdentity(val)
		if err != nil {
			return fmt.Errorf("invalid identity: %w", err)
		}
	}
	return nil
}
