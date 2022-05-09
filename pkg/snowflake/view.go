package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
)

const SnowflakeView = "snowflake_view"

type View struct {
	Comment      sql.NullString `db:"comment"`
	IsSecure     bool           `db:"is_secure"`
	Name         sql.NullString `db:"name"`
	SchemaName   sql.NullString `db:"schema_name"`
	Text         sql.NullString `db:"text"`
	DatabaseName sql.NullString `db:"database_name"`
}

func (v View) Address() string {
	return fmt.Sprintf("%v.%v", SnowflakeView, strings.ToLower(v.Name.String))
}
func (v View) ID() string {
	return fmt.Sprintf("%v|%v|%v", v.DatabaseName.String, v.SchemaName.String, v.Name.String)
}

// // Show returns the SQL query that will show the row representing this view.
// func (vb *ViewBuilder) Show() string {
// 	return fmt.Sprintf(`SHOW VIEWS LIKE '%v' IN SCHEMA "%v"."%v"`, vb.name, vb.db, vb.schema)
// }

func (v View) HCL() []byte {
	panic("not implemented")
}

// func GeneratePipeImport(pipe Pipe) string {
// 	return GenerateTFImport(SnowflakePipe, pipe.Name, PipeID(pipe))
// }

func ListViews(databaseName string, schemaName string, db *sql.DB) ([]View, error) {
	stmt := fmt.Sprintf(`SHOW VIEWS IN SCHEMA "%s"."%v"`, databaseName, schemaName)
	rows, err := Query(db, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbs := []View{}
	err = sqlx.StructScan(rows, &dbs)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no views found")
		return nil, nil
	}

	log.Println("eror", err)

	return dbs, err
}
