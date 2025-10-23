package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (qs *QueryService) performGetTickDataByTickNumberQuery(_ context.Context, tickNumber uint32) (result TickDataGetResponse, err error) {
	defer func() {
		if errors.Is(err, ErrDocumentNotFound) {
			return
		}

		if err != nil {
			qs.TotalElasticErrorCount.Add(1)
			qs.ConsecutiveElasticErrorCount.Add(1)
		} else {
			qs.ConsecutiveElasticErrorCount.Store(0)
		}
	}()

	res, err := qs.esClient.Get(qs.tickDataIndex, strconv.FormatUint(uint64(tickNumber), 10))
	if err != nil {
		return TickDataGetResponse{}, fmt.Errorf("calling es client get: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return TickDataGetResponse{}, ErrDocumentNotFound
	}

	if res.IsError() {
		return TickDataGetResponse{}, fmt.Errorf("got error response from Elasticsearch: %s", res.String())
	}

	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return TickDataGetResponse{}, fmt.Errorf("decoding response: %w", err)
	}

	return result, nil
}

func (qs *QueryService) performGetEmptyTicksQuery(ctx context.Context, startTick, endTick, epoch uint32) ([]uint32, error) {

	searchResult, err := qs.performGetTicksQuery(ctx, startTick, endTick, epoch)
	if err != nil {
		return nil, err
	}

	total := uint32(searchResult.Hits.Total.Value) // doesn't change in following queries
	numberOfEmpty := (endTick - startTick) - total
	var processed uint32
	emptyTicks := make([]uint32, 0, numberOfEmpty)

	currentTick := uint64(startTick)
	for _, hit := range searchResult.Hits.Hits {
		tickNumber, err := strconv.ParseUint(hit.Id, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tick number: %w", err)
		}
		if currentTick < tickNumber { // only the gaps are empty
			for i := currentTick; i < tickNumber; i++ {
				emptyTicks = append(emptyTicks, uint32(i))
			}
		}
		currentTick = tickNumber
		processed++
	}

	for processed < total {

		scrollId := searchResult.ScrollId
		searchResult, err = qs.performGetTicksScroll(ctx, scrollId)
		if err != nil {
			return nil, err
		}

		for _, hit := range searchResult.Hits.Hits {
			tickNumber, err := strconv.ParseUint(hit.Id, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("failed to parse tick number: %w", err)
			}
			if currentTick < tickNumber { // only the gaps are empty
				for i := currentTick; i < tickNumber; i++ {
					emptyTicks = append(emptyTicks, uint32(i))
				}
			}
			currentTick = tickNumber
			processed++
		}

	}

	return emptyTicks, nil

}

func (qs *QueryService) performGetTicksScroll(ctx context.Context, scrollId string) (*TickListSearchResponse, error) {

	res, err := qs.esClient.Scroll(
		qs.esClient.Scroll.WithContext(ctx),
		qs.esClient.Scroll.WithScrollID(scrollId),
		qs.esClient.Scroll.WithScroll(30*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("calling es client scroll: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response from elastic: %s", res.String())
	}

	var searchResult *TickListSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&searchResult); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return searchResult, nil

}

func (qs *QueryService) performGetTicksQuery(ctx context.Context, startTick, endTick, epoch uint32) (*TickListSearchResponse, error) {
	query := `{
	  "size": %d,
	  "_source": false,
	  "query": {
		"bool": {
		  "must": [
			{ "match": { "epoch": %d } },
			{ "range": { "tickNumber": { "gte": %d, "lte": %d"} } }
		  ]
		}
	  },
	  "sort": {
		"tickNumber": "asc"
	  }
	}`
	query = fmt.Sprintf(query, 10000, epoch, startTick, endTick)

	res, err := qs.esClient.Search(
		qs.esClient.Search.WithContext(ctx),
		qs.esClient.Search.WithIndex(qs.tickDataIndex),
		qs.esClient.Search.WithScroll(30*time.Second),
		qs.esClient.Search.WithSource("false"),
		qs.esClient.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		return nil, fmt.Errorf("calling es client search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response from elastic: %s", res.String())
	}

	var searchResult *TickListSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&searchResult); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return searchResult, nil
}

type TickListSearchResponse struct {
	ScrollId string `json:"_scroll_id"`
	Took     int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Hits     struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			Id string `json:"_id"`
		} `json:"hits"`
	} `json:"hits"`
}

type TickDataGetResponse struct {
	Index       string   `json:"_index"`
	Id          string   `json:"_id"`
	Version     int      `json:"_version"`
	SeqNo       int      `json:"_seq_no"`
	PrimaryTerm int      `json:"_primary_term"`
	Found       bool     `json:"found"`
	Source      TickData `json:"_source"`
}

type TickData struct {
	ComputorIndex     uint32   `json:"computorIndex"`
	Epoch             uint32   `json:"epoch"`
	TickNumber        uint32   `json:"tickNumber"`
	Timestamp         uint64   `json:"timestamp"`
	VarStruct         string   `json:"varStruct"`
	TimeLock          string   `json:"timeLock"`
	TransactionHashes []string `json:"transactionHashes"`
	ContractFees      []int64  `json:"contractFees"`
	Signature         string   `json:"signature"`
}
