package filters

import (
	"fmt"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/grpc/utils"
)

const (
	EventFilterSource                = "source"
	EventFilterDestination           = "destination"
	EventFilterTransactionHash       = "transactionHash"
	EventFilterTickNumber            = "tickNumber"
	EventFilterLogType               = "logType"
	EventFilterEpoch                 = "epoch"
	EventFilterAmount                = "amount"
	EventFilterNumberOfShares        = "numberOfShares"
	EventRangeTimestamp              = "timestamp"
	EventFilterCategories            = "categories"
	EventFilterLogId                 = "logId"
	EventFilterAssetName             = "assetName"
	EventFilterAssetIssuer           = "assetIssuer"
	EventFilterManagingContractIndex = "managingContractIndex"
	EventFilterContractIndex         = "contractIndex"
	EventFilterContractMessageType   = "contractMessageType"
	EventFilterDeductedAmount        = "deductedAmount"
	EventFilterRemainingAmount       = "remainingAmount"
	EventFilterCustomMessage         = "customMessage"
)

const maxValuesPerEventFilter = 5
const maxValueLengthPerEventIdentityFilter = 5*60 + 5 + 4 // 5 IDs + comma + optional spaces

var AllowedEventIncludeFilters = map[string]bool{
	EventFilterSource:                true,
	EventFilterDestination:           true,
	EventFilterTransactionHash:       true,
	EventFilterTickNumber:            true,
	EventFilterEpoch:                 true,
	EventFilterAmount:                true,
	EventFilterNumberOfShares:        true,
	EventFilterLogType:               true,
	EventFilterCategories:            true,
	EventFilterLogId:                 true,
	EventFilterAssetName:             true,
	EventFilterAssetIssuer:           true,
	EventFilterManagingContractIndex: true,
	EventFilterContractIndex:         true,
	EventFilterContractMessageType:   true,
	EventFilterDeductedAmount:        true,
	EventFilterRemainingAmount:       true,
	EventFilterCustomMessage:         true,
}

var AllowedEventExcludeFilters = map[string]bool{
	EventFilterSource:      true,
	EventFilterDestination: true,
}

var AllowedEventShouldFilters = map[string]bool{
	EventFilterSource:         true,
	EventFilterDestination:    true,
	EventFilterAmount:         true,
	EventFilterNumberOfShares: true,
}

var AllowedEventRanges = map[string]bool{
	EventFilterTickNumber:      true,
	EventFilterEpoch:           true,
	EventFilterAmount:          true,
	EventFilterNumberOfShares:  true,
	EventRangeTimestamp:        true,
	EventFilterDeductedAmount:  true,
	EventFilterRemainingAmount: true,
}

var AllowedEventShouldRanges = map[string]bool{
	EventFilterAmount:         true,
	EventFilterNumberOfShares: true,
}

func CreateEventFilters(filterMap map[string]string, allowedKeys map[string]bool) (map[string][]string, error) {

	res := make(map[string][]string)
	for k, v := range filterMap {

		maxValues := getMaxValuesForKey(k)
		maxLength := getMaxLengthForKey(k)

		vs, err := CreateFilters(v, maxValues, maxLength)
		if err != nil {
			return nil, fmt.Errorf("handling filter [%s]: %w", k, err)
		}
		res[k] = vs
	}

	err := validateEventsFilters(res, allowedKeys)
	if err != nil {
		return nil, fmt.Errorf("validating filter: %w", err)
	}

	return res, nil
}

type filterValidator func(values []string) error

var eventFilterValidators = map[string]filterValidator{
	EventFilterSource:                func(v []string) error { return ValidateIdentityFilterValues(v, maxValuesPerEventFilter) },
	EventFilterDestination:           func(v []string) error { return ValidateIdentityFilterValues(v, maxValuesPerEventFilter) },
	EventFilterTransactionHash:       func(v []string) error { return ValidateTransactionHashFilterValues(v, 1) },
	EventFilterTickNumber:            func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 32, 1) },
	EventFilterEpoch:                 func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 32, 1) },
	EventFilterAmount:                func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 64, 1) },
	EventFilterNumberOfShares:        func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 64, 1) },
	EventFilterLogId:                 func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 64, 1) },
	EventFilterManagingContractIndex: func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 64, 1) },
	EventFilterContractIndex:         func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 64, 1) },
	EventFilterContractMessageType:   func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 64, 1) },
	EventFilterDeductedAmount:        func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 64, 1) },
	EventFilterCustomMessage:         func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 64, 1) },
	EventFilterLogType:               func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 8, maxValuesPerEventFilter) },
	EventFilterCategories:            func(v []string) error { return ValidateUnsignedNumericFilterValues(v, 8, maxValuesPerEventFilter) },
	EventFilterAssetName:             func(v []string) error { return ValidateStringFilterLength(v, 7, 1) },
	EventFilterAssetIssuer:           func(v []string) error { return ValidateIdentityFilterValues(v, 1) },
	EventFilterRemainingAmount:       func(v []string) error { return ValidateSignedNumericFilterValue(v, 64, 1) },
}

func validateEventsFilters(filterMap map[string][]string, allowedKeys map[string]bool) error {
	if len(filterMap) == 0 {
		return nil
	}

	if len(filterMap) > len(allowedKeys) {
		return fmt.Errorf("too many filters (%d)", len(filterMap))
	}

	for key, values := range filterMap {
		if _, ok := allowedKeys[key]; !ok {
			return fmt.Errorf("unsupported filter [%s]", key)
		}

		validator, ok := eventFilterValidators[key]
		if !ok {
			return fmt.Errorf("unhandled filter: [%s]", key)
		}

		if err := validator(values); err != nil {
			return fmt.Errorf("invalid [%s] filter: %w", key, err)
		}
	}
	return nil
}

func CreateEventRanges(ranges map[string]*api.Range, allowedKeys map[string]bool) (map[string][]entities.Range, error) {
	convertedRanges := map[string][]entities.Range{}
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(ranges) > len(allowedKeys) {
		return nil, fmt.Errorf("too many ranges (%d)", len(ranges))
	}

	for key, value := range ranges {

		if _, ok := allowedKeys[key]; !ok {
			return nil, fmt.Errorf("unsupported filter [%s]", key)
		}

		switch key {
		case EventFilterAmount, EventFilterNumberOfShares, EventRangeTimestamp, EventFilterDeductedAmount:
			r, err := CreateUnsignedNumericRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid [%s] range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case EventFilterRemainingAmount:
			r, err := CreateSignedNumericRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid [%s] range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case EventFilterTickNumber, EventFilterEpoch:
			r, err := CreateUnsignedNumericRange(value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid [%s] range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		default:
			return nil, fmt.Errorf("unhandled range: [%s]", key)
		}
	}

	return convertedRanges, nil
}

const maxNumberOfShouldFilters = 2

func CreateShouldFilters(should []*api.ShouldFilter, allowedFilters, allowedRanges map[string]bool) ([]entities.ShouldFilter, error) {
	if len(should) > maxNumberOfShouldFilters {
		return nil, fmt.Errorf("too many should filters (%d)", len(should))
	}
	var shouldFilters = make([]entities.ShouldFilter, 0, len(should))
	for _, shouldFilter := range should {
		shouldFilterTerms, err := CreateEventFilters(shouldFilter.GetTerms(), allowedFilters)
		if err != nil {
			return nil, fmt.Errorf("creating filters: %w", err)
		}
		shouldFilterRanges, err := CreateEventRanges(shouldFilter.GetRanges(), allowedRanges)
		if err != nil {
			return nil, fmt.Errorf("creating ranges: %w", err)
		}
		if len(shouldFilterTerms)+len(shouldFilterRanges) < 2 {
			return nil, fmt.Errorf("needs at least two filters")
		}
		shouldFilters = append(shouldFilters, entities.ShouldFilter{
			Terms:  shouldFilterTerms,
			Ranges: shouldFilterRanges,
		})
	}
	return shouldFilters, nil
}

func getMaxValuesForKey(k string) int {
	shouldSplit := checkIfMultivalueKey(k)
	maxValues := utils.If(shouldSplit, maxValuesPerEventFilter, 1)
	return maxValues
}

func getMaxLengthForKey(k string) int {
	maxLength := utils.If(k == EventFilterTransactionHash || k == EventFilterAssetIssuer, 60, 20)
	if k == EventFilterSource || k == EventFilterDestination {
		maxLength = maxValueLengthPerEventIdentityFilter
	}
	return maxLength
}

func checkIfMultivalueKey(k string) bool {
	return k == EventFilterSource || k == EventFilterDestination || k == EventFilterLogType || k == EventFilterCategories
}
