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
func (p Pipe) ID() string {
	return PipeID(p)
}

func GeneratePipe(pipe Pipe) string {
	return buildTerraformHelper(SnowflakePipe, pipe.Name).
		SetAttributeString("name", pipe.Name).
		SetAttributeString("database", pipe.DatabaseName).
		SetAttributeString("schema", pipe.SchemaName).
		SetAttributeString("comment", pipe.Comment).
		SetAttributeString("copy_statement", pipe.Definition).
		SetAttributeBool("auto_ingest", !pipe.Integration.Valid). // Integration is NULL if auto_ingest is true
		String()
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

func GeneratePipeImport(pipe Pipe) string {
	return GenerateTFImport(SnowflakePipe, pipe.Name, PipeID(pipe))
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
