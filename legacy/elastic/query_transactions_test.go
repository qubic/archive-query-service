package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_QueryTransactionByHash(t *testing.T) {
	tests := []struct {
		name       string
		mockResp   any
		mockStatus int
		expectedTx Tx
		wantErr    error
	}{
		{
			name: "Success",
			mockResp: TransactionsSearchResponse{Hits: struct {
				Total struct {
					Value    int    `json:"value"`
					Relation string `json:"relation"`
				} `json:"total"`
				Hits []TxHit `json:"hits"`
			}{Hits: []TxHit{{Source: Tx{Hash: "abc", Amount: 100}}}}},
			mockStatus: http.StatusOK,
			expectedTx: Tx{Hash: "abc", Amount: 100},
		},
		{
			name:       "Not Found",
			mockResp:   TransactionsSearchResponse{},
			mockStatus: http.StatusOK,
			wantErr:    ErrDocumentNotFound,
		},
		{
			name:       "Error",
			mockResp:   map[string]string{"error": "fail"},
			mockStatus: http.StatusInternalServerError,
			wantErr:    fmt.Errorf("error response from Elasticsearch"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newMockClient(t, tt.mockResp, tt.mockStatus)
			res, err := c.QueryTransactionByHash(context.Background(), "abc")
			if tt.wantErr != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedTx, res.Source)
			}
		})
	}
}

func TestClient_QueryTickTransactions(t *testing.T) {
	tests := []struct {
		name       string
		mockResp   any
		mockStatus int
		wantCount  int
		wantErr    bool
	}{
		{
			name: "Success",
			mockResp: TransactionsSearchResponse{Hits: struct {
				Total struct {
					Value    int    `json:"value"`
					Relation string `json:"relation"`
				} `json:"total"`
				Hits []TxHit `json:"hits"`
			}{Hits: []TxHit{{Source: Tx{Hash: "1"}}, {Source: Tx{Hash: "2"}}}}},
			mockStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "No Transactions Found",
			mockResp:   TransactionsSearchResponse{},
			mockStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "Elastic Error",
			mockResp:   nil,
			mockStatus: http.StatusInternalServerError,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newMockClient(t, tt.mockResp, tt.mockStatus)
			res, err := c.QueryTickTransactions(context.Background(), 123)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantCount, len(res.Hits.Hits))
			}
		})
	}
}

func newMockClient(t *testing.T, resp any, status int) *Client {
	mockTrans := &mockTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			body, _ := json.Marshal(resp)
			return &http.Response{
				StatusCode: status,
				Body:       io.NopCloser(bytes.NewReader(body)),
				Header:     http.Header{"X-Elastic-Product": []string{"Elasticsearch"}},
			}, nil
		},
	}
	es, err := elasticsearch.NewClient(elasticsearch.Config{Transport: mockTrans})
	require.NoError(t, err)
	return NewElasticClient("tx", "tick", "comp", es)
}
