package rpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"slices"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pkg/errors"
	"github.com/qubic/archive-query-service/legacy/elastic"
	"github.com/qubic/archive-query-service/legacy/protobuf"
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

type StartConfig struct {
	ListenAddrGRPC string
	ListenAddrHTTP string
	MaxRecvMsgSize int
	MaxSendMsgSize int
}

type Server struct {
	protobuf.UnimplementedTransactionsServiceServer
	qb            *QueryService
	statusService statusPb.StatusServiceClient
}

func NewServer(qb *QueryService, statusClient statusPb.StatusServiceClient) *Server {
	return &Server{
		qb:            qb,
		statusService: statusClient,
	}
}

const maxPageSize uint32 = 250
const defaultPageSize uint32 = 100

func (s *Server) GetIdentityTransactions(ctx context.Context, req *protobuf.GetIdentityTransactionsRequest) (*protobuf.GetIdentityTransactionsResponse, error) {
	var pageSize uint32
	switch {
	case req.GetPageSize() > maxPageSize:
		return nil, status.Errorf(codes.InvalidArgument, "Invalid page size %d (maximum is %d).", req.GetPageSize(), maxPageSize)
	case req.GetPageSize() == 0:
		pageSize = defaultPageSize
	default:
		pageSize = req.GetPageSize()
	}

	pageNumber := max(0, int(req.Page)-1) // API index starts with '1', implementation index starts with '0'.
	if uint32(pageNumber)*pageSize+pageSize > 10000 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid pagination information. Page number and page size exceeds maximum result size of 10000.")
	}
	response, err := s.qb.performIdentitiesTransactionsQuery(ctx, req.Identity, int(pageSize), pageNumber, req.Desc, 0, 0)
	if err != nil {
		log.Printf("Error performing identities transactions query (get identity transactions): %v.", err)
		return nil, status.Errorf(codes.Internal, "performing identities transactions query.")
	}

	var transactions = make([]*protobuf.NewTransaction, 0)
	for _, hit := range response.Hits.Hits {
		transactions = append(transactions, TxToNewFormat(hit.Source))
	}

	pagination, err := getPaginationInformation(response.Hits.Total.Value, pageNumber+1, int(pageSize))
	if err != nil {
		log.Printf("Error creating pagination info: %v", err)
		return nil, status.Error(codes.Internal, "creating pagination info")
	}

	return &protobuf.GetIdentityTransactionsResponse{
		Pagination:   pagination,
		Transactions: transactions,
	}, nil

}

func (s *Server) GetIdentityTransfersInTickRangeV2(ctx context.Context, req *protobuf.GetTransferTransactionsPerTickRequestV2) (*protobuf.GetIdentityTransfersInTickRangeResponseV2, error) {
	var pageSize uint32
	switch {
	case req.GetPageSize() > maxPageSize:
		return nil, status.Errorf(codes.InvalidArgument, "Invalid page size %d (maximum is %d).", req.GetPageSize(), maxPageSize) // max size
	case req.GetPageSize() == 0:
		pageSize = defaultPageSize // default
	default:
		pageSize = req.GetPageSize()
	}
	pageNumber := max(0, int(req.Page)-1) // API index starts with '1', implementation index starts with '0'.
	if uint32(pageNumber)*pageSize+pageSize > 10000 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid pagination information. Page number and page size exceeds maximum result size of 10000.")
	}
	response, err := s.qb.performIdentitiesTransactionsQuery(ctx, req.Identity, int(pageSize), pageNumber, req.Desc, req.StartTick, req.EndTick)
	if err != nil {
		log.Printf("Error performing identities transactions query (get identity transfers): %v.", err)
		return nil, status.Error(codes.Internal, "performing identities transactions query")
	}

	totalTransfers := make([]*protobuf.PerTickIdentityTransfers, 0, len(response.Hits.Hits))

	for _, hit := range response.Hits.Hits {
		tx, err := TxToArchiveFullFormat(hit.Source)
		if err != nil {
			log.Printf("Error converting transactions to archive full format: %v", err)
			return nil, status.Error(codes.Internal, "converting transactions to output format")
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
		log.Printf("Error performing tick transactions query: %v.", err)
		return nil, status.Error(codes.Internal, "getting tick transactions")
	}

	if req.Approved {
		return s.getApprovedTickTransactionsV2(ctx, res)
	}

	if req.Transfers {
		return s.getTransferTickTransactionsV2(ctx, res)
	}

	return s.getAllTickTransactionsV2(ctx, res)

}

func (s *Server) GetTickTransactions(ctx context.Context, req *protobuf.GetTickTransactionsRequest) (*protobuf.GetTickTransactionsResponse, error) {
	res, err := s.qb.performTickTransactionsQuery(ctx, req.TickNumber)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tick transactions for specified tick not found")
		}
		log.Printf("Error performing tick transactions query: %v.", err)
		return nil, status.Error(codes.Internal, "getting tick transactions")
	}

	var transactions []*protobuf.Transaction

	for _, hit := range res.Hits.Hits {
		txData, err := TxToArchivePartialFormat(hit.Source)
		if err != nil {
			log.Printf("Error converting transaction to archive partial format: %v.", err)
			return nil, status.Errorf(codes.Internal, "converting transaction to output format")
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponse{Transactions: transactions}, nil
}

func (s *Server) GetTickApprovedTransactions(ctx context.Context, req *protobuf.GetTickApprovedTransactionsRequest) (*protobuf.GetTickApprovedTransactionsResponse, error) {
	res, err := s.qb.performTickTransactionsQuery(ctx, req.TickNumber)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Error(codes.NotFound, "tick approved transactions for specified tick not found")
		}
		log.Printf("Error performing tick transactions query: %v.", err)
		return nil, status.Error(codes.Internal, "getting tick approved transactions")
	}

	var transactions []*protobuf.Transaction

	for _, hit := range res.Hits.Hits {
		tx := hit.Source
		if !tx.MoneyFlew {
			continue
		}

		txData, err := TxToArchivePartialFormat(hit.Source)
		if err != nil {
			log.Printf("Error converting transaction to archive partial format: %v.", err)
			return nil, status.Error(codes.Internal, "converting transaction to output format")
		}

		if tx.InputType == 1 && tx.InputSize == 1000 && tx.Destination == "EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVWRF" {
			moneyFlew, err := recomputeSendManyMoneyFlew(txData)
			if err != nil {
				log.Printf("Error recomputing send many money flew: %v", err)
				return nil, status.Error(codes.Internal, "recomputing send many money flew")
			}

			if !moneyFlew {
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
		if errors.Is(err, elastic.ErrDocumentNotFound) {
			return nil, status.Errorf(codes.NotFound, "transaction with specified ID not found")
		}
		log.Printf("Error performing get transaction by id query: %v.", err)
		return nil, status.Errorf(codes.Internal, "getting transaction")
	}

	tx, err := TxToArchivePartialFormat(res.Source)
	if err != nil {
		log.Printf("Error converting transaction to archive partial format: %v", err)
		return nil, status.Errorf(codes.Internal, "converting transaction to archive partial format")
	}

	return &protobuf.GetTransactionResponse{Transaction: tx}, nil
}

func (s *Server) GetTransactionStatus(ctx context.Context, req *protobuf.GetTransactionRequest) (*protobuf.GetTransactionStatusResponse, error) {
	res, err := s.qb.performGetTxByIDQuery(ctx, req.TxId)
	if err != nil {
		if errors.Is(err, elastic.ErrDocumentNotFound) {
			return nil, status.Errorf(codes.NotFound, "transaction with specified ID not found")
		}
		log.Printf("Error performing get transaction by id query: %v.", err)
		return nil, status.Errorf(codes.Internal, "performing get transaction by id query")
	}

	if !res.Found {
		return nil, status.Errorf(codes.NotFound, "tx status for specified tx id not found")
	}

	moneyFlew := res.Source.MoneyFlew

	// this was ported from archiver, will disable it for now as I believe it's not impacting anything
	// if res.Source.Amount <= 0 {
	//	return nil, status.Errorf(codes.NotFound, "tx status for specified tx id not found")
	// }

	if !moneyFlew {
		return &protobuf.GetTransactionStatusResponse{TransactionStatus: &protobuf.TransactionStatus{TxId: res.Source.Hash, MoneyFlew: false}}, nil
	}

	txData, err := TxToArchiveFullFormat(res.Source)
	if err != nil {
		log.Printf("Error converting transaction to archive full format: %v.", err)
		return nil, status.Errorf(codes.Internal, "converting transaction to output format")
	}

	tx := txData.Transaction

	if tx.InputType == 1 && tx.InputSize == 1000 && tx.DestId == "EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVWRF" {
		moneyFlew, err = recomputeSendManyMoneyFlew(tx)
		if err != nil {
			log.Printf("Error recomputing send many money flew: %v", err)
			return nil, status.Error(codes.Internal, "recomputing send many money flew")
		}

		return &protobuf.GetTransactionStatusResponse{TransactionStatus: &protobuf.TransactionStatus{TxId: tx.TxId, MoneyFlew: moneyFlew}}, nil
	}

	return &protobuf.GetTransactionStatusResponse{TransactionStatus: &protobuf.TransactionStatus{TxId: tx.TxId, MoneyFlew: moneyFlew}}, nil
}

func (s *Server) GetTransactionV2(ctx context.Context, req *protobuf.GetTransactionRequest) (*protobuf.GetTransactionResponseV2, error) {
	res, err := s.qb.performGetTxByIDQuery(ctx, req.TxId)
	if err != nil {
		if errors.Is(err, elastic.ErrDocumentNotFound) {
			return nil, status.Errorf(codes.NotFound, "transaction with specified ID not found")
		}
		log.Printf("Error performing get transaction by id query: %v.", err)
		return nil, status.Errorf(codes.Internal, "performing get transaction by id query")
	}

	tx, err := TxToArchiveFullFormat(res.Source)
	if err != nil {
		log.Printf("Error converting transaction to archive full format: %v", err)
		return nil, status.Errorf(codes.Internal, "converting transaction to output format")
	}

	return &protobuf.GetTransactionResponseV2{Transaction: tx.Transaction, Timestamp: tx.Timestamp, MoneyFlew: tx.MoneyFlew}, nil
}

func (s *Server) GetTickData(ctx context.Context, req *protobuf.GetTickDataRequest) (*protobuf.GetTickDataResponse, error) {
	res, err := s.qb.performGetTickDataByTickNumberQuery(ctx, req.TickNumber)
	if err != nil {
		// empty tick condition
		if errors.Is(err, elastic.ErrDocumentNotFound) {
			return &protobuf.GetTickDataResponse{TickData: nil}, nil
		}
		log.Printf("Error performing get tick data by tick number query: %v.", err)
		return nil, status.Errorf(codes.Internal, "error querying tick data")
	}

	tickData, err := TickDataToArchiveFormat(res.Source)
	if err != nil {
		log.Printf("Error converting tick data to archive format: %v", err)
		return nil, status.Errorf(codes.Internal, "converting tick data to output format")
	}

	return &protobuf.GetTickDataResponse{TickData: tickData}, nil
}

func (s *Server) getApprovedTickTransactionsV2(_ context.Context, res elastic.TransactionsSearchResponse) (*protobuf.GetTickTransactionsResponseV2, error) {
	var transactions []*protobuf.TransactionData

	for _, hit := range res.Hits.Hits {
		tx := hit.Source
		if !tx.MoneyFlew {
			continue
		}

		txData, err := TxToArchiveFullFormat(hit.Source)
		if err != nil {
			log.Printf("Error converting transaction to archive full format: %v", err)
			return nil, status.Errorf(codes.Internal, "converting transaction to output format")
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
}

func (s *Server) getTransferTickTransactionsV2(_ context.Context, res elastic.TransactionsSearchResponse) (*protobuf.GetTickTransactionsResponseV2, error) {
	var transactions []*protobuf.TransactionData

	for _, hit := range res.Hits.Hits {
		tx := hit.Source
		if tx.Amount == 0 {
			continue
		}

		txData, err := TxToArchiveFullFormat(hit.Source)
		if err != nil {
			log.Printf("Error converting transaction to archive full format: %v", err)
			return nil, status.Errorf(codes.Internal, "converting transaction to output format")
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
}

func (s *Server) getAllTickTransactionsV2(_ context.Context, res elastic.TransactionsSearchResponse) (*protobuf.GetTickTransactionsResponseV2, error) {

	var transactions []*protobuf.TransactionData

	for _, hit := range res.Hits.Hits {
		txData, err := TxToArchiveFullFormat(hit.Source)
		if err != nil {
			log.Printf("Error converting transaction to archive full format: %v", err)
			return nil, status.Errorf(codes.Internal, "converting transaction to output format")
		}

		transactions = append(transactions, txData)
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
}

func (s *Server) GetArchiverStatus(ctx context.Context, empty *emptypb.Empty) (*protobuf.GetArchiverStatusResponse, error) {
	archiverStatus, err := s.statusService.GetArchiverStatus(ctx, empty)
	if err != nil {
		log.Printf("Error getting archiver status: %v", err)
		return nil, status.Errorf(codes.Internal, "getting status")
	}

	response, err := convertArchiverStatus(archiverStatus)
	if err != nil {
		log.Printf("Error converting archiver status: %v", err)
		return nil, status.Errorf(codes.Internal, "converting status")
	}

	return response, nil
}

func (s *Server) GetComputorsList(ctx context.Context, req *protobuf.GetComputorsRequest) (*protobuf.GetComputorsResponse, error) {
	response, err := s.qb.performComputorListByEpochQuery(ctx, req.Epoch)
	if err != nil {
		log.Printf("Error performing get computor list by epoch query: %v.", err)
		return nil, status.Errorf(codes.Internal, "performing computors list query")
	}

	if response.Hits.Total.Value == 0 {
		return nil, status.Errorf(codes.NotFound, "computors list for specified epoch not found")
	}

	hit := response.Hits.Hits[0]
	computorsList, err := ComputorsListToArchiveFormat(hit.Source)
	if err != nil {
		log.Printf("Error converting computor list to archive format: %v", err)
		return nil, status.Errorf(codes.Internal, "converting computors list to output format")
	}

	return &protobuf.GetComputorsResponse{Computors: computorsList}, nil
}

func (s *Server) GetLatestTick(ctx context.Context, _ *emptypb.Empty) (*protobuf.GetLatestTickResponse, error) {
	maxTick, err := s.qb.cache.GetMaxTick(ctx)
	if err != nil {
		log.Printf("Error getting max tick: %v", err)
		return nil, status.Errorf(codes.Internal, "fetching last processed tick")
	}

	return &protobuf.GetLatestTickResponse{
		LatestTick: maxTick,
	}, nil
}

const maxTickListPageSize = int32(1000)

func (s *Server) GetEpochTickListV2(ctx context.Context, request *protobuf.GetEpochTickListRequestV2) (*protobuf.GetEpochTickListResponseV2, error) {

	if request.PageSize > maxTickListPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "invalid page size %d exceeds maximum %d.", request.PageSize, maxTickListPageSize)
	}

	page := max(1, request.Page)
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = 10
	}

	intervals, err := s.qb.cache.GetTickIntervals(ctx)
	if err != nil {
		log.Printf("[ERROR] getting tick intervals: %v", err)
		return nil, internalErrorGettingTickIntervals()
	}

	if len(intervals) == 0 {
		log.Println("[ERROR] no tick intervals found.")
		return nil, internalErrorGettingTickIntervals()
	}

	if request.Epoch+1 < intervals[len(intervals)-1].Epoch {
		// log.Printf("[DEBUG] Get ticks: invalid epoch: [%d]", request.Epoch)
		return nil, status.Errorf(codes.InvalidArgument, "Requested epoch too old. Only current epoch-1 is supported.")
	} else if request.Epoch > intervals[len(intervals)-1].Epoch {
		// log.Printf("[DEBUG] Get ticks: invalid epoch: [%d]", request.Epoch)
		return nil, status.Errorf(codes.InvalidArgument, "Requested epoch is in the future.")
	}

	var count uint32
	filteredIntervals := make([]*statusPb.TickInterval, 0)
	for _, interval := range intervals {
		if request.Epoch == interval.Epoch {
			count += (interval.LastTick + 1) - interval.FirstTick
			filteredIntervals = append(filteredIntervals, interval)
		}
	}

	emptyTicks, err := s.qb.GetEmptyTicks(ctx, request.Epoch, filteredIntervals)
	if err != nil {
		log.Printf("[ERROR] getting empty ticks for epoch [%d]: %v", request.Epoch, err)
		return nil, internalErrorGettingTickIntervals()
	}

	start := uint32((page - 1) * pageSize)
	end := min(start+uint32(pageSize), count)
	ticks := make([]*protobuf.Tick, 0, pageSize)

	if request.GetDesc() {
		ticks = getTickListReversedPageData(filteredIntervals, start, end, ticks, emptyTicks)
	} else {
		ticks = getTickListPageData(filteredIntervals, start, end, ticks, emptyTicks)
	}

	pagination, err := getPaginationInformation(int(count), int(page), int(pageSize))
	if err != nil {
		log.Printf("Error creating pagination info: %v", err)
		return nil, internalErrorGettingTickIntervals()
	}

	return &protobuf.GetEpochTickListResponseV2{
		Pagination: pagination,
		Ticks:      ticks,
	}, nil

}

func getTickListPageData(filteredIntervals []*statusPb.TickInterval, start uint32, end uint32, ticks []*protobuf.Tick, emptyTicks *EmptyTicks) []*protobuf.Tick {
	processed := uint32(0)
	for _, interval := range filteredIntervals {
		for i := interval.FirstTick; i <= interval.LastTick; i++ {
			if processed >= start && processed < end {
				ticks = append(ticks, &protobuf.Tick{
					TickNumber: i,
					IsEmpty:    emptyTicks.Ticks[i],
				})
			}
			processed++
		}
	}
	return ticks
}

func getTickListReversedPageData(filteredIntervals []*statusPb.TickInterval, start uint32, end uint32, ticks []*protobuf.Tick, emptyTicks *EmptyTicks) []*protobuf.Tick {
	processed := uint32(0)
	slices.Reverse(filteredIntervals)
	for _, interval := range filteredIntervals {
		for i := interval.LastTick; i >= interval.FirstTick; i-- {
			if processed >= start && processed < end {
				ticks = append(ticks, &protobuf.Tick{
					TickNumber: i,
					IsEmpty:    emptyTicks.Ticks[i],
				})
			}
			processed++
		}
	}
	return ticks
}

func (s *Server) GetEmptyTickListV2(ctx context.Context, request *protobuf.GetEpochEmptyTickListRequestV2) (*protobuf.GetEpochEmptyTickListResponseV2, error) {

	if request.PageSize > maxTickListPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "invalid page size %d exceeds maximum %d.", request.PageSize, maxTickListPageSize)
	}

	page := max(1, request.Page)
	pageSize := request.PageSize
	if pageSize == 0 {
		pageSize = 10
	}

	intervals, err := s.qb.cache.GetTickIntervals(ctx)
	if err != nil {
		log.Printf("[ERROR] getting tick intervals: %v", err)
		return nil, internalErrorGettingTickIntervals()
	}

	if len(intervals) == 0 {
		log.Println("[ERROR] no tick intervals found.")
		return nil, internalErrorGettingTickIntervals()
	}

	if request.Epoch+1 < intervals[len(intervals)-1].Epoch {
		// log.Printf("[DEBUG] Get empty ticks: invalid epoch: [%d]", request.Epoch)
		return nil, status.Errorf(codes.InvalidArgument, "Requested epoch too old. Only current epoch-1 is supported.")
	} else if request.Epoch > intervals[len(intervals)-1].Epoch {
		// log.Printf("[DEBUG] Get empty ticks: invalid epoch: [%d]", request.Epoch)
		return nil, status.Errorf(codes.InvalidArgument, "Requested epoch is in the future.")
	}

	filteredIntervals := make([]*statusPb.TickInterval, 0)
	for _, interval := range intervals {
		if request.Epoch == interval.Epoch {
			filteredIntervals = append(filteredIntervals, interval)
		}
	}

	emptyTicks, err := s.qb.GetEmptyTicks(ctx, request.Epoch, filteredIntervals)
	if err != nil {
		log.Printf("[ERROR] getting empty ticks for epoch [%d]: %v", request.Epoch, err)
		return nil, internalErrorGettingTickIntervals()
	}

	emptyTicksList := make([]uint32, 0, len(emptyTicks.Ticks))
	for tickNumber := range emptyTicks.Ticks {
		emptyTicksList = append(emptyTicksList, tickNumber)
	}
	slices.Sort(emptyTicksList) // the ticks are not sorted before this point

	count := uint32(len(emptyTicksList))
	start := uint32((page - 1) * pageSize)
	end := min(start+uint32(pageSize), count)

	pagination, err := getPaginationInformation(int(count), int(page), int(pageSize))
	if err != nil {
		log.Printf("Error creating pagination info: %v", err)
		return nil, internalErrorGettingTickIntervals()
	}

	return &protobuf.GetEpochEmptyTickListResponseV2{
		Pagination: pagination,
		EmptyTicks: emptyTicksList[min(start, count):end],
	}, nil

}

func internalErrorGettingTickIntervals() error {
	return status.Error(codes.Internal, "getting tick intervals")
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
		totalPages++
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
		return false, fmt.Errorf("decoding tx input: %w", err)
	}
	var sendManyPayload types.SendManyTransferPayload
	err = sendManyPayload.UnmarshallBinary(decodedInput)
	if err != nil {
		return false, fmt.Errorf("unmarshalling payload: %w", err)
	}

	if tx.Amount < sendManyPayload.GetTotalAmount() {
		return false, nil
	}

	return true, nil
}

func (s *Server) Start(cfg StartConfig, interceptors ...grpc.UnaryServerInterceptor) error {

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.MaxSendMsgSize),
		grpc.ChainUnaryInterceptor(interceptors...),
	)
	protobuf.RegisterTransactionsServiceServer(srv, s)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", cfg.ListenAddrGRPC)
	if err != nil {
		return fmt.Errorf("listening on grpc port: %w", err)
	}

	go func() {
		if err := srv.Serve(lis); err != nil {
			panic(err)
		}
	}()

	if cfg.ListenAddrHTTP != "" {
		go func() {
			mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
				MarshalOptions: protojson.MarshalOptions{EmitDefaultValues: true, EmitUnpopulated: true},
			}))
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize),
					grpc.MaxCallSendMsgSize(cfg.MaxSendMsgSize),
				),
			}
			if err := protobuf.RegisterTransactionsServiceHandlerFromEndpoint(
				context.Background(),
				mux,
				cfg.ListenAddrGRPC,
				opts,
			); err != nil {
				panic(err)
			}

			if err := http.ListenAndServe(cfg.ListenAddrHTTP, mux); err != nil {
				panic(err)
			}
		}()
	}

	return nil
}
