package elastic

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/qubic/archive-query-service/v2/entities"
)

func getFilterStrings(filters map[string][]string) []string {
	keys := getSortedKeys(filters) // sort for a deterministic filter order

	filterStrings := make([]string, 0, len(filters))
	for _, k := range keys {
		esField := k
		if k == "logType" {
			esField = "type"
		}
		if len(filters[k]) > 1 {
			filterStrings = append(filterStrings, fmt.Sprintf(`{"terms":{"%s":["%s"]}}`, esField, strings.Join(filters[k], `","`)))
		} else if len(filters[k]) == 1 {
			filterStrings = append(filterStrings, fmt.Sprintf(`{"term":{"%s":"%s"}}`, esField, filters[k][0]))
		}
	}
	return filterStrings
}

func getRangeFilterStrings(ranges map[string][]entities.Range) ([]string, error) {
	filterStrings := make([]string, 0, len(ranges))
	keys := getSortedKeys(ranges) // sort for a deterministic filter order
	for _, k := range keys {
		esField := k
		if k == "logType" {
			esField = "type"
		}
		rangeString, err := createRangeFilter(esField, ranges[k])
		if err != nil {
			log.Printf("error computing range filter [%s]: %v", k, ranges[k])
			return nil, fmt.Errorf("creating range filter: %w", err)
		}
		filterStrings = append(filterStrings, rangeString)
	}
	return filterStrings, nil
}

func getShouldFilterStrings(shouldFilters []entities.ShouldFilter) ([]string, error) {
	filterStrings := make([]string, 0, len(shouldFilters)*2)

	for _, should := range shouldFilters {

		termFilters := getFilterStrings(should.Terms)
		rangeFilters, err := getRangeFilterStrings(should.Ranges)
		if err != nil {
			return nil, fmt.Errorf("getting range filters: %w", err)
		}

		if len(termFilters) > 0 || len(rangeFilters) > 0 {
			/*
				{ "bool": { "should": [ ... terms and ranges ... ], "minimum_should_match": 1 } },
			*/
			filterStrings = append(filterStrings,
				fmt.Sprintf(`{"bool":{"should":[%s], "minimum_should_match": 1}}`,
					strings.Join(append(termFilters, rangeFilters...), ",")))
		}

	}

	return filterStrings, nil
}

func createRangeFilter(property string, r []entities.Range) (string, error) {
	var rangeStrings []string
	for _, v := range r {
		rangeStrings = append(rangeStrings, fmt.Sprintf(`"%s":"%s"`, v.Operation, v.Value))
	}
	if len(rangeStrings) > 0 {
		return fmt.Sprintf(`{"range":{"%s":{%s}}}`, property, strings.Join(rangeStrings, ",")), nil
	}

	return "", fmt.Errorf("computing range for [%s]", property)
}

func getSortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
