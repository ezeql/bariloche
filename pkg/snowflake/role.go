package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const SnowflakeRole = "snowflake_role"

type Role struct {
	Name    sql.NullString `db:"name"`
	Comment sql.NullString `db:"comment"`
}

func (r Role) Address() string {
	return fmt.Sprintf("%v.%v", SnowflakeRole, strings.ToLower(r.Name.String))
}

func (r Role) ID() string {
	return r.Name.String
}

func ListRoles(db *sql.DB) ([]Role, error) {
	stmt := "SHOW ROLES"
	rows, err := Query(db, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	roles := []Role{}
	err = sqlx.StructScan(rows, &roles)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no roles found")
		return nil, nil
	}
	return roles, errors.Wrapf(err, "unable to scan row for %s", stmt)
}

func GenerateRole(role Role) string {
	return buildTerraformHelper(SnowflakeRole, role.Name.String).
		SetAttributeNullString("name", role.Name).
		SetAttributeNullString("comment", role.Comment).String()
}
