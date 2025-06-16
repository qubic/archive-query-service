package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	api "github.com/qubic/archive-query-service/v2/api/archive-query-service/v2"
	"github.com/qubic/archive-query-service/v2/domain"
	"strconv"
)

type tickDataGetResponse struct {
	Index       string   `json:"_index"`
	Id          string   `json:"_id"`
	Version     int      `json:"_version"`
	SeqNo       int      `json:"_seq_no"`
	PrimaryTerm int      `json:"_primary_term"`
	Found       bool     `json:"found"`
	Source      tickData `json:"_source"`
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

func (r *Repository) GetTickData(_ context.Context, tickNumber uint32) (*api.TickData, error) {
	res, err := r.esClient.Get(r.tickDataIndex, strconv.FormatUint(uint64(tickNumber), 10))
	if err != nil {
		return nil, fmt.Errorf("calling es client get: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, domain.ErrNotFound
	}

	if res.IsError() {
		return nil, fmt.Errorf("got error response from Elasticsearch: %s", res.String())
	}

	var result tickDataGetResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return tickDataToApiTickData(result.Source), nil
}
