package rpc

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pkg/errors"
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

var ErrNotFound = errors.New("store resource not found")

var _ protobuf.TransactionsServiceServer = &Server{}

const MaxTickCacheKey = "max-tick"

type Server struct {
	protobuf.UnimplementedTransactionsServiceServer
	listenAddrGRPC string
	listenAddrHTTP string
	qb             *QueryBuilder
}

func NewServer(listenAddrGRPC, listenAddrHTTP string, qb *QueryBuilder) *Server {
	return &Server{
		listenAddrGRPC: listenAddrGRPC,
		listenAddrHTTP: listenAddrHTTP,
		qb:             qb,
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
	response, err := s.qb.performIdentitiesTransactionsQuery(ctx, req.Identity, int(pageSize), pageNumber, req.Desc)
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
	response, err := s.qb.performIdentitiesTransactionsQuery(ctx, req.Identity, int(pageSize), pageNumber, req.Desc)
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

func (s *Server) GetTickTransactionsV2(ctx context.Context, req *protobuf.GetTickTransactionsRequestV2) (*protobuf.GetTickTransactionsResponseV2, error) {
	res, err := s.qb.performTickTransactionsQuery(ctx, req.TickNumber)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tick transfer transactions for specified tick not found")
		}
		return nil, status.Errorf(codes.Internal, "getting tick transactions: %v", err)
	}

	if req.Approved {
		return s.GetApprovedTickTransactionsV2(ctx, res)
	}

	if req.Transfers {
		return s.GetTransferTickTransactionsV2(ctx, res)
	}

	return s.GetAllTickTransactionsV2(ctx, res)

}

func (s *Server) GetApprovedTickTransactionsV2(ctx context.Context, res TransactionsSearchResponse) (*protobuf.GetTickTransactionsResponseV2, error) {
	var transactions []*protobuf.TransactionData

	for _, hit := range res.Hits.Hits {
		tx := hit.Source
		if !tx.MoneyFlew {
			continue
		}

		inputBytes, err := base64.StdEncoding.DecodeString(tx.InputData)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decoding base64 input for tx with id %s", tx.Hash)
		}

		sigBytes, err := base64.StdEncoding.DecodeString(tx.Signature)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decoding base64 signature for tx with id %s", tx.Hash)
		}

		txData := &protobuf.TransactionData{
			Transaction: &protobuf.TransactionData_Transaction{
				SourceId:     tx.Source,
				DestId:       tx.Destination,
				Amount:       tx.Amount,
				TickNumber:   tx.TickNumber,
				InputType:    tx.InputType,
				InputSize:    tx.InputSize,
				InputHex:     hex.EncodeToString(inputBytes),
				SignatureHex: hex.EncodeToString(sigBytes),
				TxId:         tx.Hash,
			},
			Timestamp: tx.Timestamp,
			MoneyFlew: tx.MoneyFlew,
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
}

func (s *Server) GetTransferTickTransactionsV2(ctx context.Context, res TransactionsSearchResponse) (*protobuf.GetTickTransactionsResponseV2, error) {
	var transactions []*protobuf.TransactionData

	for _, hit := range res.Hits.Hits {
		tx := hit.Source
		if tx.Amount == 0 {
			continue
		}

		inputBytes, err := base64.StdEncoding.DecodeString(tx.InputData)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decoding base64 input for tx with id %s", tx.Hash)
		}

		sigBytes, err := base64.StdEncoding.DecodeString(tx.Signature)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decoding base64 signature for tx with id %s", tx.Hash)
		}

		txData := &protobuf.TransactionData{
			Transaction: &protobuf.TransactionData_Transaction{
				SourceId:     tx.Source,
				DestId:       tx.Destination,
				Amount:       tx.Amount,
				TickNumber:   tx.TickNumber,
				InputType:    tx.InputType,
				InputSize:    tx.InputSize,
				InputHex:     hex.EncodeToString(inputBytes),
				SignatureHex: hex.EncodeToString(sigBytes),
				TxId:         tx.Hash,
			},
			Timestamp: tx.Timestamp,
			MoneyFlew: tx.MoneyFlew,
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
}

func (s *Server) GetAllTickTransactionsV2(ctx context.Context, res TransactionsSearchResponse) (*protobuf.GetTickTransactionsResponseV2, error) {

	var transactions []*protobuf.TransactionData

	for _, hit := range res.Hits.Hits {
		tx := hit.Source

		inputBytes, err := base64.StdEncoding.DecodeString(tx.InputData)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decoding base64 input for tx with id %s", tx.Hash)
		}

		sigBytes, err := base64.StdEncoding.DecodeString(tx.Signature)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decoding base64 signature for tx with id %s", tx.Hash)
		}

		txData := &protobuf.TransactionData{
			Transaction: &protobuf.TransactionData_Transaction{
				SourceId:     tx.Source,
				DestId:       tx.Destination,
				Amount:       tx.Amount,
				TickNumber:   tx.TickNumber,
				InputType:    tx.InputType,
				InputSize:    tx.InputSize,
				InputHex:     hex.EncodeToString(inputBytes),
				SignatureHex: hex.EncodeToString(sigBytes),
				TxId:         tx.Hash,
			},
			Timestamp: tx.Timestamp,
			MoneyFlew: tx.MoneyFlew,
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
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

func (s *Server) Start(interceptors ...grpc.UnaryServerInterceptor) error {

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(600*1024*1024),
		grpc.MaxSendMsgSize(600*1024*1024),
		grpc.ChainUnaryInterceptor(interceptors...),
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
