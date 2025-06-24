package grpc

import (
	"fmt"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
)

const maxPageSize uint32 = 1024
const maxHitsSize uint32 = 10000

func validatePage(page *api.Page) error {
	if page.GetSize() > maxPageSize {
		return fmt.Errorf("maximum page size: [%d], got: [%d]", maxPageSize, page.GetSize())
	}
	if page.GetNumber()*page.GetSize() >= maxHitsSize {
		return fmt.Errorf("maximum result size: [%d]", maxHitsSize)
	}
	return nil
}

func validateTransactionFilters(filters *api.GetTransactionsForIdentityFilters) error {
	var err error
	if filters == nil {
		return nil
	}
	if filters.Destination != nil {
		err = validateIdentity(filters.GetDestination())
	}
	return err
}

func validateTransactionAggregations(filters *api.GetTransactionsForIdentityFilters, aggregations *api.GetTransactionsForIdentityAggregations) error {
	var err error
	if aggregations == nil {
		return nil
	}
	if filters != nil {
		if aggregations.Amount != nil && filters.Amount != nil {
			return fmt.Errorf("only one of filter or aggregation must be specified for [amount]")
		}
		if aggregations.TickNumber != nil && filters.TickNumber != nil {
			return fmt.Errorf("only one of filter or aggregation must be specified for [tickNumber]")
		}
		if aggregations.InputType != nil && filters.InputType != nil {
			return fmt.Errorf("only one of filter or aggregation must be specified for [inputType]")
		}
		if aggregations.Timestamp != nil && filters.Timestamp != nil {
			return fmt.Errorf("only one of filter or aggregation must be specified for [timestamp]")
		}
	}
	return err
}

func validateIdentity(identity string) error {
	// FIXME implement - move validation code from middleware.go
	return nil
}
