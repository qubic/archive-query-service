package filters

import (
	"fmt"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/entities"
	"github.com/qubic/archive-query-service/v2/grpc/utils"
)

const (
	EventFilterSource          = "source"
	EventFilterDestination     = "destination"
	EventFilterTransactionHash = "transactionHash"
	EventFilterTickNumber      = "tickNumber"
	EventFilterLogType         = "logType"
	EventFilterEpoch           = "epoch"
	EventFilterAmount          = "amount"
	EventFilterNumberOfShares  = "numberOfShares"
	EventRangeTimestamp        = "timestamp"
	EventFilterCategories      = "categories"
	EventFilterLogId           = "logId"
)

const maxValuesPerEventFilter = 5
const maxValueLengthPerEventIdentityFilter = 5*60 + 5 + 4 // 5 IDs + comma + optional spaces

var AllowedEventIncludeFilters = map[string]bool{
	EventFilterSource:          true,
	EventFilterDestination:     true,
	EventFilterTransactionHash: true,
	EventFilterTickNumber:      true,
	EventFilterEpoch:           true,
	EventFilterAmount:          true,
	EventFilterNumberOfShares:  true,
	EventFilterLogType:         true,
	EventFilterCategories:      true,
	EventFilterLogId:           true,
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
	EventFilterTickNumber:     true,
	EventFilterEpoch:          true,
	EventFilterAmount:         true,
	EventFilterNumberOfShares: true,
	EventRangeTimestamp:       true,
}

var AllowedEventShouldRanges = map[string]bool{
	EventFilterAmount:         true,
	EventFilterNumberOfShares: true,
}

func CreateEventFilters(filterMap map[string]string, allowedKeys map[string]bool) (map[string][]string, error) {

	res := make(map[string][]string)
	for k, v := range filterMap {

		shouldSplit := k == EventFilterSource || k == EventFilterDestination || k == EventFilterLogType || k == EventFilterCategories

		maxValues := utils.If(shouldSplit, maxValuesPerEventFilter, 1)
		maxLength := utils.If(k == EventFilterTransactionHash, 60, 15)
		if k == EventFilterSource || k == EventFilterDestination {
			maxLength = maxValueLengthPerEventIdentityFilter
		}

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

		switch key {
		case EventFilterSource, EventFilterDestination:

			err := ValidateIdentityFilterValues(values, maxValuesPerEventFilter)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case EventFilterTransactionHash:

			err := ValidateTransactionHashFilterValues(values, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case EventFilterTickNumber, EventFilterEpoch:

			err := ValidateUnsignedNumericFilterValues(values, 32, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case EventFilterAmount, EventFilterNumberOfShares, EventFilterLogId:

			err := ValidateUnsignedNumericFilterValues(values, 64, 1)
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		case EventFilterLogType, EventFilterCategories:

			err := ValidateUnsignedNumericFilterValues(values, 8, maxValuesPerEventFilter) // uint8 <= 255
			if err != nil {
				return fmt.Errorf("invalid [%s] filter: %w", key, err)
			}

		default:
			return fmt.Errorf("unhandled filter: [%s]", key)
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
		case EventFilterAmount, EventFilterNumberOfShares, EventRangeTimestamp:
			r, err := CreateNumericRange(value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid [%s] range: %w", key, err)
			}
			if len(r) > 0 {
				convertedRanges[key] = r
			}
		case EventFilterTickNumber, EventFilterEpoch:
			r, err := CreateNumericRange(value, 32)
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
