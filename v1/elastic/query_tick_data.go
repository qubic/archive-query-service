package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func (c *Client) QueryTickDataByTickNumber(ctx context.Context, tickNumber uint32) (result TickDataGetResponse, err error) {

	res, err := c.elastic.Get(
		c.tickDataIndex,
		strconv.FormatUint(uint64(tickNumber), 10),
		c.elastic.Get.WithContext(ctx),
	)
	if err != nil {
		return TickDataGetResponse{}, fmt.Errorf("calling es client get: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("[WARN] failed to close body: %v", err)
		}
	}(res.Body)

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

func (c *Client) QueryEmptyTicks(ctx context.Context, startTick, endTick, epoch uint32) ([]uint32, error) {

	searchResult, err := c.performGetTicksQuery(ctx, startTick, endTick, epoch)
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
		searchResult, err = c.performGetTicksScroll(ctx, scrollId)
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

func (c *Client) performGetTicksQuery(ctx context.Context, startTick, endTick, epoch uint32) (*TickListSearchResponse, error) {
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

	return executeSearch(func() (*esapi.Response, error) {
		return c.elastic.Search(
			c.elastic.Search.WithContext(ctx),
			c.elastic.Search.WithIndex(c.tickDataIndex),
			c.elastic.Search.WithScroll(30*time.Second),
			c.elastic.Search.WithSource("false"),
			c.elastic.Search.WithBody(strings.NewReader(query)),
		)
	})
}

func (c *Client) performGetTicksScroll(ctx context.Context, scrollId string) (*TickListSearchResponse, error) {
	return executeSearch(func() (*esapi.Response, error) {
		return c.elastic.Scroll(
			c.elastic.Scroll.WithContext(ctx),
			c.elastic.Scroll.WithScrollID(scrollId),
			c.elastic.Scroll.WithScroll(30*time.Second),
		)
	})
}

func executeSearch(search func() (*esapi.Response, error)) (*TickListSearchResponse, error) {
	res, err := search()
	if err != nil {
		return nil, fmt.Errorf("calling es client search: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("[WARN] failed to close body: %v", err)
		}
	}(res.Body)

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
