package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTransport struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

func TestClient_QueryEmptyTicks(t *testing.T) {
	tests := []struct {
		name           string
		startTick      uint32
		endTick        uint32
		epoch          uint32
		mockResponses  []*TickListSearchResponse
		expectedResult []uint32
		expectedErr    string
	}{
		{
			name:           "All ticks are empty",
			startTick:      10,
			endTick:        15,
			epoch:          1,
			mockResponses:  []*TickListSearchResponse{{Hits: createHits(0, []string{})}},
			expectedResult: []uint32{10, 11, 12, 13, 14, 15},
			expectedErr:    "",
		},
		{
			name:           "Some ticks are empty (gaps)",
			startTick:      10,
			endTick:        15,
			epoch:          1,
			mockResponses:  []*TickListSearchResponse{{Hits: createHits(3, []string{"11", "13", "14"})}},
			expectedResult: []uint32{10, 12, 15},
			expectedErr:    "",
		},
		{
			name:           "No ticks are empty",
			startTick:      10,
			endTick:        12,
			epoch:          1,
			mockResponses:  []*TickListSearchResponse{{Hits: createHits(3, []string{"10", "11", "12"})}},
			expectedResult: []uint32{},
			expectedErr:    "",
		},
		{
			name:      "Pagination with Scroll",
			startTick: 10,
			endTick:   20,
			epoch:     1,
			mockResponses: []*TickListSearchResponse{
				{ScrollID: "scroll123", Hits: createHits(4, []string{"10", "12"})},
				{Hits: createHits(4, []string{"14", "15"})},
			},
			expectedResult: []uint32{11, 13, 16, 17, 18, 19, 20},
			expectedErr:    "",
		},
		{
			name:      "Elasticsearch error",
			startTick: 10,
			endTick:   15,
			epoch:     1,
			mockResponses: []*TickListSearchResponse{
				nil,
			},
			expectedResult: nil,
			expectedErr:    "error response from elastic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responseIdx := 0
			mockTrans := &mockTransport{
				roundTripFunc: func(req *http.Request) (*http.Response, error) {
					if responseIdx >= len(tt.mockResponses) {
						return nil, io.EOF
					}
					resp := tt.mockResponses[responseIdx]
					responseIdx++

					header := make(http.Header)
					header.Set("X-Elastic-Product", "Elasticsearch")

					if resp == nil {
						return &http.Response{
							StatusCode: http.StatusInternalServerError,
							Body:       io.NopCloser(bytes.NewReader([]byte(`{"error": "internal server error"}`))),
							Header:     header,
						}, nil
					}

					body, _ := json.Marshal(resp)
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(bytes.NewReader(body)),
						Header:     header,
					}, nil
				},
			}

			esClient, err := elasticsearch.NewClient(elasticsearch.Config{
				Transport: mockTrans,
			})
			require.NoError(t, err)

			client := NewElasticClient("tx", "tick", "comp", esClient)

			result, err := client.QueryEmptyTicks(context.Background(), tt.startTick, tt.endTick, tt.epoch)

			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.NoError(t, err)
				if tt.expectedResult == nil {
					assert.Nil(t, result)
				} else {
					assert.Equal(t, tt.expectedResult, result)
				}
			}
		})
	}
}

func createHits(total int, ids []string) struct {
	Total struct {
		Value    int    `json:"value"`
		Relation string `json:"relation"`
	} `json:"total"`
	Hits []struct {
		ID string `json:"_id"`
	} `json:"hits"`
} {
	hits := make([]struct {
		ID string `json:"_id"`
	}, len(ids))
	for i, id := range ids {
		hits[i].ID = id
	}

	return struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			ID string `json:"_id"`
		} `json:"hits"`
	}{
		Total: struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		}{Value: total},
		Hits: hits,
	}
}
