package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const SnowflakeDatabase = "snowflake_database"

func (db Database) Address() string {
	return fmt.Sprintf("%v.%v", SnowflakeDatabase, strings.ToLower(db.DBName.String))
}

func (db Database) ID() string {
	return db.DBName.String
}

func (db Database) HCL() []byte {
	return buildTerraformHelper(SnowflakeDatabase, db.DBName.String).
		SetAttributeString("name", db.DBName.String).
		SetAttributeString("comment", db.Comment.String).
		File.Bytes()
}

type Database struct {
	CreatedOn     sql.NullString `db:"created_on"`
	DBName        sql.NullString `db:"name"`
	IsDefault     sql.NullString `db:"is_default"`
	IsCurrent     sql.NullString `db:"is_current"`
	Origin        sql.NullString `db:"origin"`
	Owner         sql.NullString `db:"owner"`
	Comment       sql.NullString `db:"comment"`
	Options       sql.NullString `db:"options"`
	RetentionTime sql.NullString `db:"retention_time"`
}

func ListDatabases(sdb *sqlx.DB) ([]Database, error) {
	stmt := "SHOW DATABASES"
	rows, err := sdb.Queryx(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbs := []Database{}
	err = sqlx.StructScan(rows, &dbs)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no databases found")
		return nil, nil
	}
	return dbs, errors.Wrapf(err, "unable to scan row for %s", stmt)
}
