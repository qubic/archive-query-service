package elastic

import (
	"context"
	"fmt"
	"log"
	"strings"

	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/domain"
)

type tickDataSearchResponse struct {
	Hits struct {
		Hits []struct {
			Source tickData `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type tickData struct {
	ComputorIndex     uint32   `json:"computorIndex"`
	Epoch             uint32   `json:"epoch"`
	TickNumber        uint32   `json:"tickNumber"`
	Timestamp         uint64   `json:"timestamp"`
	VarStruct         string   `json:"varStruct"`
	Timelock          string   `json:"timeLock"`
	TransactionHashes []string `json:"transactionHashes"`
	ContractFees      []int64  `json:"contractFees"`
	Signature         string   `json:"signature"`
}

// GetTickData Returns the tick data or domain.ErrNotFound if there is not tick data for this tick number.
func (r *ArchiveRepository) GetTickData(ctx context.Context, tickNumber uint32) (*api.TickData, error) {
	query := createTickDataByTickNumberQuery(tickNumber)

	var searchRes tickDataSearchResponse
	err := performElasticSearch(ctx, r.esClient, r.tickDataIndex, strings.NewReader(query), &searchRes)
	if err != nil {
		return nil, fmt.Errorf("performing elastic search: %w", err)
	}

	if len(searchRes.Hits.Hits) == 0 {
		return nil, domain.ErrNotFound
	}

	if len(searchRes.Hits.Hits) > 1 { // should never happen. same tick number in multiple indices.
		log.Printf("[WARN] multiple tick data results found for tick [%d].", tickNumber)
	}

	return tickDataToAPITickData(searchRes.Hits.Hits[0].Source), nil
}

func createTickDataByTickNumberQuery(tickNumber uint32) string {
	return fmt.Sprintf(`{"query":{"term":{"tickNumber":"%d"}},"size":1}`, tickNumber)
}
