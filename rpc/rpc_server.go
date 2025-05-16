package rpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/jellydator/ttlcache/v3"
	"github.com/qubic/archive-query-service/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"log"
	"net"
	"net/http"
	"sync/atomic"
)

var _ protobuf.TransactionsServiceServer = &Server{}

const MaxTickCacheKey = "max-tick"

type Server struct {
	protobuf.UnimplementedTransactionsServiceServer
	listenAddrGRPC               string
	listenAddrHTTP               string
	esClient                     *elasticsearch.Client
	ConsecutiveElasticErrorCount atomic.Int32
	TotalElasticErrorCount       atomic.Int32
	StatusServiceUrl             string
	cache                        *ttlcache.Cache[string, uint32]
}

func NewServer(listenAddrGRPC, listenAddrHTTP string, esClient *elasticsearch.Client, statusServiceUrl string, cache *ttlcache.Cache[string, uint32]) *Server {

	return &Server{
		listenAddrGRPC:   listenAddrGRPC,
		listenAddrHTTP:   listenAddrHTTP,
		esClient:         esClient,
		StatusServiceUrl: statusServiceUrl,
		cache:            cache,
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
	response, err := s.performIdentitiesTransactionsQuery(ctx, s.esClient, req.Identity, int(pageSize), pageNumber, req.Desc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "performing identities transactions query: %s", err.Error())
	}

	var transactions []*protobuf.Transaction
	for _, hit := range response.Hits.Hits {
		transactions = append(transactions, &protobuf.Transaction{
			SourceId:   hit.Source.Source,
			DestId:     hit.Source.Destination,
			Amount:     hit.Source.Amount,
			TickNumber: hit.Source.TickNumber,
			InputType:  hit.Source.InputType,
			InputSize:  hit.Source.InputSize,
			Input:      hit.Source.InputData,
			Signature:  hit.Source.Signature,
			TxId:       hit.Source.Hash,
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

func (s *Server) GetIdentityTransfersInTickRangeV2(ctx context.Context, req *protobuf.GetTransferTransactionsPerTickRequestV2) (*protobuf.GetIdentityTransfersInTickRangeResponseV2, error) {
	var pageSize uint32
	if req.GetPageSize() > maxPageSize { // max size
		return nil, status.Errorf(codes.InvalidArgument, "Invalid page size (maximum is %d).", maxPageSize)
	} else if req.GetPageSize() == 0 {
		pageSize = defaultPageSize // default
	} else {
		pageSize = req.GetPageSize()
	}
	pageNumber := max(0, int(req.Page)-1) // API index starts with '1', implementation index starts with '0'.
	response, err := s.performIdentitiesTransactionsQuery(ctx, s.esClient, req.Identity, int(pageSize), pageNumber, req.Desc)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "performing identities transactions query: %s", err.Error())
	}

	totalTransfers := make([]*protobuf.PerTickIdentityTransfers, 0, len(response.Hits.Hits))

	for _, hit := range response.Hits.Hits {
		inputBytes, err := base64.StdEncoding.DecodeString(hit.Source.InputData)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decoding base64 input for tx with id %s", hit.Source.Hash)
		}

		sigBytes, err := base64.StdEncoding.DecodeString(hit.Source.Signature)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decoding base64 signature for tx with id %s", hit.Source.Hash)
		}

		perTickIdentityTransfers := &protobuf.PerTickIdentityTransfers{
			TickNumber: hit.Source.TickNumber,
			Identity:   req.Identity,
			Transactions: []*protobuf.TransactionData{
				{
					Transaction: &protobuf.TransactionData_Transaction{
						SourceId:     hit.Source.Source,
						DestId:       hit.Source.Destination,
						Amount:       hit.Source.Amount,
						TickNumber:   hit.Source.TickNumber,
						InputType:    hit.Source.InputType,
						InputSize:    hit.Source.InputSize,
						InputHex:     hex.EncodeToString(inputBytes),
						SignatureHex: hex.EncodeToString(sigBytes),
						TxId:         hit.Source.Hash,
					},
					Timestamp: hit.Source.Timestamp,
					MoneyFlew: hit.Source.MoneyFlew,
				},
			},
		}
		totalTransfers = append(totalTransfers, perTickIdentityTransfers)
	}

	pagination, err := getPaginationInformation(response.Hits.Total.Value, pageNumber+1, int(pageSize))
	if err != nil {
		log.Printf("Error creating pagination info: %s", err.Error())
		return nil, status.Error(codes.Internal, "creating pagination info")
	}

	return &protobuf.GetIdentityTransfersInTickRangeResponseV2{
		Pagination:   pagination,
		Transactions: totalTransfers,
	}, nil

}

func createIdentitiesQuery(ID string, pageSize, pageNumber int, desc bool, maxTick uint32) (bytes.Buffer, error) {
	from := pageNumber * pageSize
	querySort := "asc"
	if desc {
		querySort = "desc"
	}

	tickNumberRangeFilter := map[string]interface{}{
		"lte": maxTick,
		//"gte": 10000000, // min tick can also be implemented if needed
	}
	if maxTick <= 0 {
		delete(tickNumberRangeFilter, "lte")
	}

	query := map[string]interface{}{
		"track_total_hits": "true",
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []interface{}{
					map[string]interface{}{
						"match": map[string]interface{}{
							"source": ID,
						},
					},
					map[string]interface{}{
						"match": map[string]interface{}{
							"destination": ID,
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"tickNumber": tickNumberRangeFilter,
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

func (s *Server) performIdentitiesTransactionsQuery(ctx context.Context, esClient *elasticsearch.Client, ID string, pageSize, pageNumber int, desc bool) (EsSearchResponse, error) {

	var maxTick uint32

	if s.cache.Has(MaxTickCacheKey) {

		item := s.cache.Get(MaxTickCacheKey)
		maxTick = item.Value()
	} else {

		httpMaxTick, err := s.fetchStatusMaxTick(ctx)
		if err != nil {
			return EsSearchResponse{}, fmt.Errorf("fetching status service max tick: %v", err)
		}

		s.cache.Set(MaxTickCacheKey, httpMaxTick, ttlcache.DefaultTTL)
		item := s.cache.Get(MaxTickCacheKey)
		maxTick = item.Value()
	}

	query, err := createIdentitiesQuery(ID, pageSize, pageNumber, desc, maxTick)
	if err != nil {
		return EsSearchResponse{}, fmt.Errorf("creating query: %v", err)
	}

	fmt.Println(maxTick)

	res, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex("qubic-transactions-alias"),
		esClient.Search.WithBody(&query),
		esClient.Search.WithPretty(),
	)
	if err != nil {
		s.TotalElasticErrorCount.Add(1)
		s.ConsecutiveElasticErrorCount.Add(1)
		return EsSearchResponse{}, fmt.Errorf("performing search: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		s.TotalElasticErrorCount.Add(1)
		s.ConsecutiveElasticErrorCount.Add(1)
		return EsSearchResponse{}, fmt.Errorf("error response from Elasticsearch: %s", res.String())
	}

	// Decode the response into a map.
	var result EsSearchResponse
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		return EsSearchResponse{}, fmt.Errorf("decoding response: %v", err)
	}

	s.ConsecutiveElasticErrorCount.Store(0)

	return result, nil
}

func (s *Server) fetchStatusMaxTick(ctx context.Context) (uint32, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.StatusServiceUrl, nil)
	if err != nil {
		return 0, fmt.Errorf("creating new request: %v", err)
	}

	httpRes, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("requesting max tick from status service: %v", err)
	}
	defer httpRes.Body.Close()

	body, err := io.ReadAll(httpRes.Body)
	if err != nil {
		return 0, fmt.Errorf("reading request body: %v", err)
	}

	var statusResponse struct {
		LastProcessedTick uint32 `json:"lastProcessedTick"`
	}

	err = json.Unmarshal(body, &statusResponse)
	if err != nil {
		return 0, fmt.Errorf("unmarshalling response: %v", err)
	}

	return statusResponse.LastProcessedTick, nil
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
	Hash        string `json:"hash"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      int64  `json:"amount"`
	TickNumber  uint32 `json:"tickNumber"`
	InputType   uint32 `json:"inputType"`
	InputSize   uint32 `json:"inputSize"`
	InputData   string `json:"inputData"`
	Signature   string `json:"signature"`
	Timestamp   uint64 `json:"timestamp"`
	MoneyFlew   bool   `json:"moneyFlew"`
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
