package snowflake

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const SnowflakeSchema = "snowflake_schema"

type Schema struct {
	Name          sql.NullString `db:"name"`
	DatabaseName  sql.NullString `db:"database_name"`
	Comment       sql.NullString `db:"comment"`
	Options       sql.NullString `db:"options"`
	RetentionTime sql.NullString `db:"retention_time"`
}

func (s Schema) Address() string {
	return JoinToLower(".", SnowflakeSchema, fmt.Sprintf("%v_%v", s.DatabaseName.String, s.Name.String))
}

func (s Schema) ID() string {
	return fmt.Sprintf("%v|%v", s.DatabaseName.String, s.Name.String)
}

func (s Schema) ResourceName() string {
	return s.Name.String
}

func (s Schema) HCL() []byte {
	return buildTerraformHelper(SnowflakeSchema, fmt.Sprintf("%v_%v", s.DatabaseName.String, s.Name.String)).
		SetAttributeNullString("name", s.Name).
		SetAttributeNullString("database", s.DatabaseName).
		SetAttributeNullString("comment", s.Comment).File.Bytes()
}

func ListSchemas(databaseName string, db *sql.DB) ([]Schema, error) {
	stmt := fmt.Sprintf(`SHOW SCHEMAS IN DATABASE "%v"`, databaseName)
	rows, err := Query(db, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbs := []Schema{}
	err = sqlx.StructScan(rows, &dbs)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no schemas found")
		return nil, nil
	}
	return dbs, errors.Wrapf(err, "unable to scan row for %s", stmt)
}

// func GenerateRole(role Role) string {
// 	return buildTerraformHelper(SnowflakeRole, role.Name.String).
// 		SetAttributeNullString("name", role.Name).
// 		SetAttributeNullString("comment", role.Comment).String()
// }
