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

func ValidateUnsignedNumericFilterValues(values []string, bitSize, maxNumberOfValues int) error {
	err := checkQuantity(values, maxNumberOfValues)
	if err != nil {
		return err
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
	return validateDigest(values, maxValues, false)
}

func ValidateTransactionHashFilterValues(values []string, maxValues int) error {
	return validateDigest(values, maxValues, true)
}

func validateDigest(values []string, maxValues int, lowercase bool) error {
	err := checkQuantity(values, maxValues)
	if err != nil {
		return err
	}
	for _, val := range values {
		err := utils.ValidateDigest(val, lowercase)
		if err != nil {
			return fmt.Errorf("invalid transaction hash: %w", err)
		}
	}
	return nil
}

func checkQuantity(values []string, maxValues int) error {
	if len(values) == 0 || len(values) > maxValues {
		return fmt.Errorf("invalid number of values: [%d]", maxValues)
	}
	return nil
}
