package rpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pkg/errors"
	"github.com/qubic/archive-query-service/protobuf"
	statusPb "github.com/qubic/go-data-publisher/status-service/protobuf"
	"github.com/qubic/go-node-connector/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var ErrNotFound = errors.New("store resource not found")

var _ protobuf.TransactionsServiceServer = &Server{}

type Server struct {
	protobuf.UnimplementedTransactionsServiceServer
	listenAddrGRPC string
	listenAddrHTTP string
	qb             *QueryBuilder
	statusService  statusPb.StatusServiceClient
}

func NewServer(listenAddrGRPC, listenAddrHTTP string, qb *QueryBuilder, statusClient statusPb.StatusServiceClient) *Server {
	return &Server{
		listenAddrGRPC: listenAddrGRPC,
		listenAddrHTTP: listenAddrHTTP,
		qb:             qb,
		statusService:  statusClient,
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
	response, err := s.qb.performIdentitiesTransactionsQuery(ctx, req.Identity, int(pageSize), pageNumber, req.Desc, 0, 0)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "performing identities transactions query: %s", err.Error())
	}

	var transactions []*protobuf.NewTransaction
	for _, hit := range response.Hits.Hits {
		transactions = append(transactions, TxToNewFormat(hit.Source))
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
	response, err := s.qb.performIdentitiesTransactionsQuery(ctx, req.Identity, int(pageSize), pageNumber, req.Desc, req.StartTick, req.EndTick)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "performing identities transactions query: %s", err.Error())
	}

	totalTransfers := make([]*protobuf.PerTickIdentityTransfers, 0, len(response.Hits.Hits))

	for _, hit := range response.Hits.Hits {
		tx, err := TxToArchiveFullFormat(hit.Source)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting transaction to archive full format: %s", err.Error())
		}

		perTickIdentityTransfers := &protobuf.PerTickIdentityTransfers{
			TickNumber:   hit.Source.TickNumber,
			Identity:     req.Identity,
			Transactions: []*protobuf.TransactionData{tx},
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

func (s *Server) GetTickTransactions(ctx context.Context, req *protobuf.GetTickTransactionsRequest) (*protobuf.GetTickTransactionsResponse, error) {
	res, err := s.qb.performTickTransactionsQuery(ctx, req.TickNumber)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tick transactions for specified tick not found")
		}
		return nil, status.Errorf(codes.Internal, "getting tick transactions: %v", err)
	}

	var transactions []*protobuf.Transaction

	for _, hit := range res.Hits.Hits {
		txData, err := TxToArchivePartialFormat(hit.Source)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting transaction to archive partial format: %s", err.Error())
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponse{Transactions: transactions}, nil
}

func (s *Server) GetTickApprovedTransactions(ctx context.Context, req *protobuf.GetTickApprovedTransactionsRequest) (*protobuf.GetTickApprovedTransactionsResponse, error) {
	res, err := s.qb.performTickTransactionsQuery(ctx, req.TickNumber)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tick approved transactions for specified tick not found")
		}
		return nil, status.Errorf(codes.Internal, "getting tick approved transactions: %v", err)
	}

	var transactions []*protobuf.Transaction

	for _, hit := range res.Hits.Hits {
		tx := hit.Source
		if !tx.MoneyFlew {
			continue
		}

		txData, err := TxToArchivePartialFormat(hit.Source)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting transaction to archive partial format: %s", err.Error())
		}

		if tx.InputType == 1 && tx.InputSize == 1000 && tx.Destination == "EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVWRF" {
			moneyFlew, err := recomputeSendManyMoneyFlew(txData)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "recomputeSendManyMoneyFlew: %v", err)
			}

			if moneyFlew == false {
				continue
			}
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickApprovedTransactionsResponse{ApprovedTransactions: transactions}, nil
}

func (s *Server) GetTransaction(ctx context.Context, req *protobuf.GetTransactionRequest) (*protobuf.GetTransactionResponse, error) {
	res, err := s.qb.performGetTxByIDQuery(ctx, req.TxId)
	if err != nil {
		if errors.Is(err, ErrDocumentNotFound) {
			return nil, status.Errorf(codes.NotFound, "transaction with specified ID not found")
		}
		return nil, status.Errorf(codes.Internal, "getting transaction: %v", err)
	}

	tx, err := TxToArchivePartialFormat(res.Source)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting transaction to archive partial format: %s", err.Error())
	}

	return &protobuf.GetTransactionResponse{Transaction: tx}, nil
}

func (s *Server) GetTransactionStatus(ctx context.Context, req *protobuf.GetTransactionRequest) (*protobuf.GetTransactionStatusResponse, error) {
	res, err := s.qb.performGetTxByIDQuery(ctx, req.TxId)
	if err != nil {
		if errors.Is(err, ErrDocumentNotFound) {
			return nil, status.Errorf(codes.NotFound, "transaction with specified ID not found")
		}
		return nil, status.Errorf(codes.Internal, "getting transaction: %v", err)
	}

	if !res.Found {
		return nil, status.Errorf(codes.NotFound, "tx status for specified tx id not found")
	}

	moneyFlew := res.Source.MoneyFlew

	// this was ported from archiver, will disable it for now as I believe it's not impacting anything
	//if res.Source.Amount <= 0 {
	//	return nil, status.Errorf(codes.NotFound, "tx status for specified tx id not found")
	//}

	if moneyFlew == false {
		return &protobuf.GetTransactionStatusResponse{TransactionStatus: &protobuf.TransactionStatus{TxId: res.Source.Hash, MoneyFlew: false}}, nil
	}

	txData, err := TxToArchiveFullFormat(res.Source)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting transaction to archive full format: %s", err.Error())
	}

	tx := txData.Transaction

	if tx.InputType == 1 && tx.InputSize == 1000 && tx.DestId == "EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVWRF" {
		moneyFlew, err = recomputeSendManyMoneyFlew(tx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "recomputeSendManyMoneyFlew: %v", err)
		}

		return &protobuf.GetTransactionStatusResponse{TransactionStatus: &protobuf.TransactionStatus{TxId: tx.TxId, MoneyFlew: moneyFlew}}, nil
	}

	return &protobuf.GetTransactionStatusResponse{TransactionStatus: &protobuf.TransactionStatus{TxId: tx.TxId, MoneyFlew: moneyFlew}}, nil
}

func (s *Server) GetTransactionV2(ctx context.Context, req *protobuf.GetTransactionRequest) (*protobuf.GetTransactionResponseV2, error) {
	res, err := s.qb.performGetTxByIDQuery(ctx, req.TxId)
	if err != nil {
		if errors.Is(err, ErrDocumentNotFound) {
			return nil, status.Errorf(codes.NotFound, "transaction with specified ID not found")
		}
		return nil, status.Errorf(codes.Internal, "getting transaction: %v", err)
	}

	tx, err := TxToArchiveFullFormat(res.Source)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting transaction to archive full format: %s", err.Error())
	}

	return &protobuf.GetTransactionResponseV2{Transaction: tx.Transaction, Timestamp: tx.Timestamp, MoneyFlew: tx.MoneyFlew}, nil
}

func (s *Server) GetTickData(ctx context.Context, req *protobuf.GetTickDataRequest) (*protobuf.GetTickDataResponse, error) {
	res, err := s.qb.performGetTickDataByTickNumberQuery(ctx, req.TickNumber)
	if err != nil {
		// empty tick condition
		if errors.Is(err, ErrDocumentNotFound) {
			return &protobuf.GetTickDataResponse{TickData: nil}, nil
		}

		return nil, status.Errorf(codes.Internal, "getting tick data: %v", err)
	}

	tickData, err := TickDataToArchiveFormat(res.Source)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting tick data to archive format: %s", err.Error())
	}

	return &protobuf.GetTickDataResponse{TickData: tickData}, nil
}

func (s *Server) GetApprovedTickTransactionsV2(ctx context.Context, res TransactionsSearchResponse) (*protobuf.GetTickTransactionsResponseV2, error) {
	var transactions []*protobuf.TransactionData

	for _, hit := range res.Hits.Hits {
		tx := hit.Source
		if !tx.MoneyFlew {
			continue
		}

		txData, err := TxToArchiveFullFormat(hit.Source)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting transaction to archive full format: %s", err.Error())
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

		txData, err := TxToArchiveFullFormat(hit.Source)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting transaction to archive full format: %s", err.Error())
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
}

func (s *Server) GetAllTickTransactionsV2(ctx context.Context, res TransactionsSearchResponse) (*protobuf.GetTickTransactionsResponseV2, error) {

	var transactions []*protobuf.TransactionData

	for _, hit := range res.Hits.Hits {
		txData, err := TxToArchiveFullFormat(hit.Source)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "converting transaction to archive full format: %s", err.Error())
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
}

func (s *Server) GetArchiverStatus(ctx context.Context, empty *emptypb.Empty) (*protobuf.GetArchiverStatusResponse, error) {
	archiverStatus, err := s.statusService.GetArchiverStatus(ctx, empty)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting status: %s", err.Error())
	}

	response, err := convertArchiverStatus(archiverStatus)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting status: %s", err.Error())
	}

	return response, nil
}

func (s *Server) GetComputorsList(ctx context.Context, req *protobuf.GetComputorsRequest) (*protobuf.GetComputorsResponse, error) {
	response, err := s.qb.performComputorListByEpochQuery(ctx, req.Epoch)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "performing computors list query: %s", err.Error())
	}

	if response.Hits.Total.Value == 0 {
		return nil, status.Errorf(codes.NotFound, "computors list for specified epoch not found")
	}

	hit := response.Hits.Hits[0]
	computorsList, err := ComputorsListToArchiveFormat(hit.Source)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "converting computors list to archive format: %s", err.Error())
	}

	return &protobuf.GetComputorsResponse{Computors: computorsList}, nil
}

func (s *Server) GetLatestTick(ctx context.Context, _ *emptypb.Empty) (*protobuf.GetLatestTickResponse, error) {
	maxTick, err := s.qb.cache.GetMaxTick(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "fetching last processed tick: %s", err.Error())
	}

	return &protobuf.GetLatestTickResponse{
		LatestTick: maxTick,
	}, nil
}

func convertArchiverStatus(source *statusPb.GetArchiverStatusResponse) (*protobuf.GetArchiverStatusResponse, error) {
	marshalled, err := proto.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("marshalling status: %s", err.Error())
	}
	var target protobuf.GetArchiverStatusResponse
	err = proto.Unmarshal(marshalled, &target)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling status: %s", err.Error())
	}
	return &target, nil
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

func recomputeSendManyMoneyFlew(tx *protobuf.Transaction) (bool, error) {
	decodedInput, err := hex.DecodeString(tx.InputHex)
	if err != nil {
		return false, status.Errorf(codes.Internal, "decoding tx input: %v", err)
	}
	var sendmanypayload types.SendManyTransferPayload
	err = sendmanypayload.UnmarshallBinary(decodedInput)
	if err != nil {
		return false, status.Errorf(codes.Internal, "unmarshalling payload: %v", err)
	}

	if tx.Amount < sendmanypayload.GetTotalAmount() {
		return false, nil
	}

	return true, nil
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
		return fmt.Errorf("listening on grpc port: %w", err)
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
