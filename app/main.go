package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	archiverpb "github.com/qubic/go-archiver/protobuff"

	"log"
	"qubic-long-term-storage/business/data/tx"
	"qubic-long-term-storage/database"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {

	cfg := database.Config{
		User:         "luk",
		Password:     "MHNCIlwlwd875*",
		Host:         "redshift-trial.cwub3fxijzyd.us-east-1.redshift.amazonaws.com:5439",
		Name:         "qubic",
		MaxOpenConns: 20,
		DisableTLS:   false,
	}

	archiverClient, err := NewArchiveClient("168.119.168.200:9001")
	if err != nil {
		return errors.Wrap(err, "creating archiver client")
	}

	// Connect to the database
	db, err := database.Open(cfg)
	if err != nil {
		return errors.Wrap(err, "opening database connection")
	}
	defer db.Close()

	status, err := archiverClient.GetStatus(context.Background(), nil)
	if err != nil {
		return errors.Wrap(err, "getting status")
	}

	for _, epochIntervals := range status.ProcessedTickIntervalsPerEpoch {
		log.Printf("Started epoch %d\n", epochIntervals.Epoch)
		for _, interval := range epochIntervals.Intervals {
			for tick := interval.InitialProcessedTick; tick <= interval.LastProcessedTick; tick++ {
				err := func(tick uint32) error {
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()

					txs, err := archiverClient.GetTickTransactions(ctx, &archiverpb.GetTickTransactionsRequest{TickNumber: tick})
					if err != nil {
						return errors.Wrap(err, "getting tick transactions")
					}

					if len(txs.Transactions) == 0 {
						return nil
					}

					txToInsert := make([]tx.Tx, 0, len(txs.Transactions))

					for _, archiveTx := range txs.Transactions {
						inputBytes, err := hex.DecodeString(archiveTx.InputHex)
						if err != nil {
							return errors.Wrap(err, "decoding input hex")
						}
						sigBytes, err := hex.DecodeString(archiveTx.SignatureHex)
						if err != nil {
							return errors.Wrap(err, "decoding signature hex")
						}
						txToInsert = append(txToInsert, tx.Tx{
							TxID:       archiveTx.TxId,
							SourceID:   archiveTx.SourceId,
							DestID:     archiveTx.DestId,
							Amount:     archiveTx.Amount,
							TickNumber: archiveTx.TickNumber,
							InputType:  archiveTx.InputType,
							InputSize:  archiveTx.InputSize,
							Input:      base64.StdEncoding.EncodeToString(inputBytes),
							Signature:  base64.StdEncoding.EncodeToString(sigBytes),
						})
					}

					err = tx.InsertTxBatch(ctx, db, txToInsert)
					if err != nil {
						return errors.Wrap(err, "inserting tx")
					}

					return nil
				}(tick)

				if err != nil {
					fmt.Printf("error processing tick %d: %v\n", tick, err)
					tick--
					continue
				}

				log.Printf("Processed tick %d\n", tick)
			}
		}
	}

	fmt.Println(status)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	txs := []tx.Tx{}

	// Insert transactions
	err = tx.InsertTxBatch(ctx, db, txs)
	if err != nil {
		return errors.Wrap(err, "inserting tx")
	}

	return nil
}

func NewArchiveClient(integrationGrpcHost string) (archiverpb.ArchiveServiceClient, error) {
	archiverConn, err := grpc.NewClient(integrationGrpcHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Wrap(err, "creating event api connection")
	}

	client := archiverpb.NewArchiveServiceClient(archiverConn)
	return client, nil
}
