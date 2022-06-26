package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const SnowflakePipe = "snowflake_pipe"

type Pipe struct {
	Createdon           string         `db:"created_on"`
	Name                string         `db:"name"`
	DatabaseName        string         `db:"database_name"`
	SchemaName          string         `db:"schema_name"`
	Definition          string         `db:"definition"`
	Owner               string         `db:"owner"`
	Comment             string         `db:"comment"`
	NotificationChannel sql.NullString `db:"notification_channel"`
	Integration         sql.NullString `db:"integration"`
	ErrorIntegration    sql.NullString `db:"error_integration"`
}

func (p Pipe) Address() string {
	return fmt.Sprintf("%v.%v", SnowflakePipe, strings.ToLower(p.Name))
}

func (p Pipe) ResourceName() string {
	return strings.ToLower(fmt.Sprintf("%v_%v", p.DatabaseName, p.Name))
}

func (p Pipe) ID() string {
	return PipeID(p)
}

func (p Pipe) HCL() []byte {
	return buildTerraformHelper(SnowflakePipe, p.Name).
		SetAttributeString("name", p.Name).
		SetAttributeString("database", p.DatabaseName).
		SetAttributeString("schema", p.SchemaName).
		SetAttributeString("comment", p.Comment).
		SetAttributeString("copy_statement", p.Definition).
		SetAttributeBool("auto_ingest", !p.Integration.Valid). // Integration is NULL if auto_ingest is true
		File.Bytes()
}

func GeneratePipeSQL(pipe Pipe) string {
	sql := strings.ReplaceAll(pipe.Definition, "\n    ", "\n")
	return sql
}
func PipeID(pipe Pipe) string {
	return fmt.Sprintf("%v|%v|%v", pipe.DatabaseName, pipe.SchemaName, pipe.Name)
}

func PipeSQLFilePath(pipe Pipe) string {
	return fmt.Sprintf("pipe_%v.sql", strings.ToLower(pipe.Name))
}

func ListPipes(databaseName string, schemaName string, db *sql.DB) ([]Pipe, error) {
	stmt := fmt.Sprintf(`SHOW PIPES IN SCHEMA "%s"."%v"`, databaseName, schemaName)
	rows, err := Query(db, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbs := []Pipe{}
	err = sqlx.StructScan(rows, &dbs)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no pipes found")
		return nil, nil
	}
	return dbs, errors.Wrapf(err, "unable to scan row for %s", stmt)
}
