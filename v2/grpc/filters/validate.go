package filters

import (
	"fmt"

	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/grpc/utils"
)

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

func VerifyNoConflictingFilters(queryFilters entities.Filters) error {
	valid, err := checkForConflictingFilters(make(map[string]bool, 10), queryFilters.Include)
	if err != nil {
		return err
	}
	valid, err = checkForConflictingFilters(valid, queryFilters.Exclude)
	if err != nil {
		return err
	}
	valid, err = checkForConflictingFilters(valid, queryFilters.Ranges)
	if err != nil {
		return err
	}
	for _, should := range queryFilters.Should {
		valid, err = checkForConflictingFilters(valid, should.Terms)
		if err != nil {
			return err
		}
		valid, err = checkForConflictingFilters(valid, should.Ranges)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkForConflictingFilters[F any](known map[string]bool, checked map[string]F) (map[string]bool, error) {
	for k := range checked {
		if _, found := known[k]; found {
			return nil, fmt.Errorf("duplicate [%s] filter", k)
		}
		known[k] = true
	}
	return known, nil
}

func checkQuantity(values []string, maxValues int) error {
	if len(values) == 0 || len(values) > maxValues {
		return fmt.Errorf("invalid number of values: [%d]", maxValues)
	}
	return nil
}
