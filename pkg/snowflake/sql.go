package snowflake

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Query will run stmt against the db and return the rows. We use
// [DB.Unsafe](https://godoc.org/github.com/jmoiron/sqlx#DB.Unsafe) so that we can scan to structs
// without worrying about newly introduced columns
func Query(db *sql.DB, stmt string) (*sqlx.Rows, error) {
	sdb := sqlx.NewDb(db, "snowflake").Unsafe()
	return sdb.Queryx(stmt)
}
