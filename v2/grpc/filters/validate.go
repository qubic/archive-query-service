package filters

import (
	"fmt"

	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/grpc/utils"
)

func ValidateUnsignedNumericFilterValues(values []string, bitSize, maxNumberOfValues int) error {
	return validateNumericFilterValue(values, bitSize, maxNumberOfValues, stringToUnsignedNumericValue)
}

func ValidateSignedNumericFilterValue(values []string, bitSize, maxNumberOfValues int) error {
	return validateNumericFilterValue(values, bitSize, maxNumberOfValues, stringToSignedNumericValue)
}

func validateNumericFilterValue[T numeric](values []string, bitSize, maxNumberOfValues int, parseFunc parseFunc[T]) error {
	err := checkQuantity(values, maxNumberOfValues)
	if err != nil {
		return err
	}
	for _, val := range values {
		_, err := parseFunc(val, bitSize)
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

func ValidateStringFilterLength(values []string, maxLength, maxNumberOfValues int) error {
	err := checkQuantity(values, maxNumberOfValues)
	if err != nil {
		return err
	}
	for _, val := range values {
		if len(val) > maxLength {
			return fmt.Errorf("invalid string length: %d", len(val))
		}
	}
	return nil
}

func validateDigest(values []string, maxValues int, lowercase bool) error {
	err := checkQuantity(values, maxValues)
	if err != nil {
		return err
	}
	for _, val := range values {
		err := utils.ValidateDigest(val, lowercase)
		if err != nil {
			return fmt.Errorf("invalid digest [%s]: %w", val, err)
		}
	}
	return nil
}

func VerifyNoConflictingFilters(queryFilters entities.Filters) error {
	keys := make(map[string]bool, 10)
	err := checkForConflictingKeys(keys, queryFilters.Include, true)
	if err != nil {
		return err
	}
	err = checkForConflictingKeys(keys, queryFilters.Ranges, true)
	if err != nil {
		return err
	}

	// we do not check the exclude filters against the should filters
	// allow excluding values that are returned by applying the should filters
	err = checkForConflictingKeys(keys, queryFilters.Exclude, false) // do not modify
	if err != nil {
		return err
	}

	for _, should := range queryFilters.Should {
		err = checkForConflictingKeys(keys, should.Ranges, true)
		if err != nil {
			return err
		}
		err = checkForConflictingKeys(keys, should.Terms, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkForConflictingKeys[F any](known map[string]bool, checked map[string]F, add bool) error {
	for k := range checked {
		if _, found := known[k]; found {
			return fmt.Errorf("duplicate [%s] filter", k)
		}
		if add {
			known[k] = true
		}
	}
	return nil
}

func checkQuantity(values []string, maxValues int) error {
	if len(values) == 0 || len(values) > maxValues {
		return fmt.Errorf("invalid number of values (%d>%d)", len(values), maxValues)
	}
	return nil
}
