package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const SnowflakeStage = "snowflake_stage"

type Stage struct {
	Name                sql.NullString `db:"name"`
	DatabaseName        sql.NullString `db:"database_name"`
	SchemaName          sql.NullString `db:"schema_name"`
	Comment             sql.NullString `db:"comment"`
	URL                 sql.NullString `db:"url"`
	StorageIntegration  sql.NullString `db:"storage_integration"`
	NotificationChannel sql.NullString `db:"notification_channel"`
}

func (s Stage) Address() string {
	return fmt.Sprintf("%v.%v", SnowflakeStage, strings.ToLower(s.Name.String))
}
func (s Stage) ID() string {
	return StageID(s)
}

func (s Stage) ResourceName() string {
	return fmt.Sprintf("%v_%v", s.DatabaseName.String, s.Name.String)
}

func (s Stage) HCL() []byte {
	return GenerateStage(s)
}

func GenerateStage(stage Stage) []byte {
	h := buildTerraformHelper(SnowflakeStage, stage.Name.String).
		SetAttributeNullString("name", stage.Name).
		SetAttributeNullString("database", stage.DatabaseName).
		SetAttributeNullString("schema", stage.SchemaName).
		SetAttributeNullString("comment", stage.Comment).
		SetAttributeNullString("storage_integration", stage.StorageIntegration).
		SetAttributeNullString("url", stage.URL)
	return h.File.Bytes()

}

func StageID(stage Stage) string {
	return fmt.Sprintf("%v|%v|%v", stage.DatabaseName.String, stage.SchemaName.String, stage.Name.String)
}

func GenerateStageImport(stage Stage) string {
	return GenerateTFImport(SnowflakeStage, stage.Name.String, StageID(stage))
}

func ListStages(databaseName string, schemaName string, db *sql.DB) ([]Stage, error) {
	stmt := fmt.Sprintf(`SHOW STAGES IN SCHEMA "%s"."%v"`, databaseName, schemaName)
	rows, err := Query(db, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbs := []Stage{}
	err = sqlx.StructScan(rows, &dbs)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no stages found")
		return nil, nil
	}
	return dbs, errors.Wrapf(err, "unable to scan row for %s", stmt)
}
