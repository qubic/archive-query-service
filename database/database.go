// Package database provides support for access the database.
package database

import (
	"context"
	"fmt"
	"net/url"

	"github.com/pkg/errors"

	_ "github.com/jackc/pgx/v5/stdlib" // The database driver in use.
	"github.com/jmoiron/sqlx"
)

// Config is the required properties to use the database.
type Config struct {
	User         string
	Password     string
	Host         string
	Name         string
	MaxOpenConns int
	DisableTLS   bool
}

// Open knows how to open a database connection based on the configuration.
func Open(cfg Config) (*sqlx.DB, error) {
	sslMode := "require"
	if cfg.DisableTLS {
		sslMode = "disable"
	}

	q := make(url.Values)
	q.Set("sslmode", sslMode)
	q.Set("timezone", "utc")

	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     cfg.Host,
		Path:     cfg.Name,
		RawQuery: q.Encode(),
	}

	db, err := sqlx.Open("pgx", u.String())
	if err != nil {
		return nil, errors.Wrap(err, "opening conn to db")
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)

	return db, nil
}

// StatusCheck returns nil if it can successfully talk to the database. It
// returns a non-nil error otherwise.
func StatusCheck(ctx context.Context, db *sqlx.DB) error {
	// Run a simple query to determine connectivity. The db has a "Ping" method
	// but it can false-positive when it was previously able to talk to the
	// database but the database has since gone away. Running this query forces a
	// round trip to the database.
	const q = `SELECT true`
	var tmp bool
	return db.QueryRowContext(ctx, q).Scan(&tmp)
}

// BuildFilterString will build a WHERE AND dynamic query based on values found in filters map
// only the first value of the filter will be used so passing values separated by comma
// q=1,3 will only use 1 for the filter
// query should look like "SELECT * from products %s "
// filter should look like {"name": "alex", "title": "hansel" }
// result will be the query with $ bindvars and the arguments list to be passed to the db queryer
// eg: query = "SELECT * FROM products WHERE 1=1 AND name = $1 AND title = $2"
// args = []interface{}{1, "3"}
func BuildFilterString(query string, filters map[string][]string, allowedFilters map[string]string) (string, []interface{}, error) {
	filterString := "WHERE 1=1"
	var inputArgs []interface{}

	for key, val := range filters {
		if realFilterName, ok := allowedFilters[key]; ok {
			if len(val) == 0 {
				continue
			}

			filterString = fmt.Sprintf("%s AND %s = ?", filterString, realFilterName)
			inputArgs = append(inputArgs, val[0])
		}
	}

	query, args, err := sqlx.In(fmt.Sprintf(query, filterString), inputArgs...)
	if err != nil {
		return "", nil, errors.Wrapf(err, "templating filters %v", filters)
	}

	query = sqlx.Rebind(sqlx.DOLLAR, query)
	return query, args, nil
}
