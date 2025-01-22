package tx

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"strings"
)

func InsertTxBatch(ctx context.Context, db *sqlx.DB, txs []Tx) error {
	if len(txs) == 0 {
		return errors.New("no transactions to insert")
	}

	// Base insert query
	query := `
		INSERT INTO transactions (
			tx_id, source_id, dest_id, amount, tick_number, input_type, input_size, input, signature
		) VALUES
	`

	// Building query placeholders
	values := []interface{}{}
	placeholders := []string{}

	for i, tx := range txs {
		start := i * 9 // 9 is the number of columns
		placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			start+1, start+2, start+3, start+4, start+5, start+6, start+7, start+8, start+9))

		values = append(values,
			tx.TxID, tx.SourceID, tx.DestID, tx.Amount,
			tx.TickNumber, tx.InputType, tx.InputSize, tx.Input, tx.Signature)
	}

	// Combine query and placeholders
	finalQuery := query + fmt.Sprintf("%s;", strings.Join(placeholders, ", "))

	// Execute the query
	stmt, err := db.PreparexContext(ctx, finalQuery)
	if err != nil {
		return errors.Wrap(err, "failed to prepare query")
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, values...)
	if err != nil {
		return errors.Wrap(err, "failed to execute query")
	}

	return nil
}
