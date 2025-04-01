package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/qubic/archive-query-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"log"
	"net"
	"net/http"
)

var _ protobuf.TransactionsServiceServer = &Server{}

type Server struct {
	protobuf.UnimplementedTransactionsServiceServer
	listenAddrGRPC string
	listenAddrHTTP string
	esClient       *elasticsearch.Client
}

func NewServer(listenAddrGRPC, listenAddrHTTP string, esClient *elasticsearch.Client) *Server {
	return &Server{
		listenAddrGRPC: listenAddrGRPC,
		listenAddrHTTP: listenAddrHTTP,
		esClient:       esClient,
	}
}

const maxPageSize uint32 = 250
const defaultPageSize uint32 = 100

func (s *Server) GetIdentityTransactions(ctx context.Context, req *protobuf.GetIdentityTransactionsRequest) (*protobuf.GetIdentityTransactionsResponse, error) {

	var pageSize uint32
	if req.GetPageSize() > maxPageSize { // max size
		return nil, status.Errorf(codes.InvalidArgument, "Invalid page size (maximum is %d).", maxPageSize)
	} else if req.GetPageSize() == 0 {
		pageSize = defaultPageSize // default
	} else {
		pageSize = req.GetPageSize()
	}
	pageNumber := max(0, int(req.Page)-1) // API index starts with '1', implementation index starts with '0'.
	response, err := performIdentitiesTransactionsQuery(ctx, s.esClient, req.Identity, int(pageSize), pageNumber, req.Desc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "performing identities transactions query: %s", err.Error())
	}

	var transactions []*protobuf.Transaction
	for _, hit := range response.Hits.Hits {
		transactions = append(transactions, &protobuf.Transaction{
			SourceId:   hit.Source.SourceID,
			DestId:     hit.Source.DestID,
			Amount:     hit.Source.Amount,
			TickNumber: hit.Source.TickNumber,
			InputType:  hit.Source.InputType,
			InputSize:  hit.Source.InputSize,
			Input:      hit.Source.Input,
			Signature:  hit.Source.Signature,
			TxId:       hit.Source.TxID,
			Timestamp:  hit.Source.Timestamp,
			MoneyFlew:  hit.Source.MoneyFlew,
		})
	}

	pagination, err := getPaginationInformation(response.Hits.Total.Value, pageNumber+1, int(pageSize))
	if err != nil {
		log.Printf("Error creating pagination info: %s", err.Error())
		return nil, status.Error(codes.Internal, "creating pagination info")
	}

	return &protobuf.GetIdentityTransactionsResponse{
		Pagination:   pagination,
		Transactions: transactions,
	}, nil

}

func createIdentitiesQuery(ID string, pageSize, pageNumber int, desc bool) (bytes.Buffer, error) {
	from := pageNumber * pageSize
	querySort := "asc"
	if desc {
		querySort = "desc"
	}
	query := map[string]interface{}{
		"track_total_hits": "true",
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []interface{}{
					map[string]interface{}{
						"match": map[string]interface{}{
							"sourceID": ID,
						},
					},
					map[string]interface{}{
						"match": map[string]interface{}{
							"destID": ID,
						},
					},
				},
				"minimum_should_match": 1,
			},
		},
		"size": pageSize,
		"from": from,
		"sort": []interface{}{
			map[string]interface{}{
				"timestamp": querySort,
			},
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return bytes.Buffer{}, fmt.Errorf("encoding query: %v", err)
	}

	return buf, nil
}

func performIdentitiesTransactionsQuery(ctx context.Context, esClient *elasticsearch.Client, ID string, pageSize, pageNumber int, desc bool) (EsSearchResponse, error) {
	query, err := createIdentitiesQuery(ID, pageSize, pageNumber, desc)
	if err != nil {
		return EsSearchResponse{}, fmt.Errorf("creating query: %v", err)
	}

	res, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex("transactions"),
		esClient.Search.WithBody(&query),
		esClient.Search.WithPretty(),
	)
	if err != nil {
		return EsSearchResponse{}, fmt.Errorf("performing search: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return EsSearchResponse{}, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Decode the response into a map.
	var result EsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return EsSearchResponse{}, fmt.Errorf("decoding response: %v", err)
	}

	return result, nil
}

type EsSearchResponse struct {
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []struct {
			Source Tx `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type Tx struct {
	TxID       string `json:"txID"`
	SourceID   string `json:"sourceID"`
	DestID     string `json:"destID"`
	Amount     int64  `json:"amount"`
	TickNumber uint32 `json:"tickNumber"`
	InputType  uint32 `json:"inputType"`
	InputSize  uint32 `json:"inputSize"`
	Input      string `json:"input"`
	Signature  string `json:"signature"`
	Timestamp  uint64 `json:"timestamp"`
	MoneyFlew  bool   `json:"moneyFlew"`
}

// ATTENTION: first page has pageNumber == 1 as API starts with index 1
func getPaginationInformation(totalRecords, pageNumber, pageSize int) (*protobuf.Pagination, error) {

	if pageNumber < 1 {
		return nil, fmt.Errorf("invalid page number [%d]", pageNumber)
	}

	if pageSize < 1 {
		return nil, fmt.Errorf("invalid page size [%d]", pageSize)
	}

	if totalRecords < 0 {
		return nil, fmt.Errorf("invalid number of total records [%d]", totalRecords)
	}

	totalPages := totalRecords / pageSize // rounds down
	if totalRecords%pageSize != 0 {
		totalPages += 1
	}

	// next page starts at index 1. -1 if no next page.
	nextPage := pageNumber + 1
	if nextPage > totalPages {
		nextPage = -1
	}

	// previous page starts at index 1. -1 if no previous page
	previousPage := pageNumber - 1
	if previousPage == 0 {
		previousPage = -1
	}

	pagination := protobuf.Pagination{
		TotalRecords: int32(totalRecords),
		CurrentPage:  int32(min(totalRecords, pageNumber)), // 0 if there are no records
		TotalPages:   int32(totalPages),                    // 0 if there are no records
		PageSize:     int32(pageSize),
		NextPage:     int32(nextPage),                      // -1 if there is none
		PreviousPage: int32(min(totalPages, previousPage)), // -1 if there is none, do not exceed total pages
	}
	return &pagination, nil
}

func (s *Server) Start() error {

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(600*1024*1024),
		grpc.MaxSendMsgSize(600*1024*1024),
	)
	protobuf.RegisterTransactionsServiceServer(srv, s)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", s.listenAddrGRPC)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		if err := srv.Serve(lis); err != nil {
			panic(err)
		}
	}()

	if s.listenAddrHTTP != "" {
		go func() {
			mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
				MarshalOptions: protojson.MarshalOptions{EmitDefaultValues: true, EmitUnpopulated: true},
			}))
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(600*1024*1024),
					grpc.MaxCallSendMsgSize(600*1024*1024),
				),
			}

			if err := protobuf.RegisterTransactionsServiceHandlerFromEndpoint(
				context.Background(),
				mux,
				s.listenAddrGRPC,
				opts,
			); err != nil {
				panic(err)
			}

			if err := http.ListenAndServe(s.listenAddrHTTP, mux); err != nil {
				panic(err)
			}
		}()
	}

	return nil
}
