package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const SnowflakeUser = "snowflake_user"

type User struct {
	Name               sql.NullString `db:"name"`
	CreatedOn          sql.NullString `db:"created_on"`
	LoginName          sql.NullString `db:"login_name"`
	DisplayName        sql.NullString `db:"display_name"`
	FirstName          sql.NullString `db:"first_name"`
	LastName           sql.NullString `db:"last_name"`
	Email              sql.NullString `db:"email"`
	MinsToUnlock       sql.NullString `db:"mins_to_unlock"`
	DaysToExpiry       sql.NullString `db:"days_to_expiry"`
	Comment            sql.NullString `db:"comment"`
	Disabled           sql.NullBool   `db:"disabled"`
	MustChangePassword sql.NullBool   `db:"must_change_password"`
	SnowflakeLock      sql.NullString `db:"snowflake_lock"`
	DefaultWarehouse   sql.NullString `db:"default_warehouse"`
	DefaultNamespace   sql.NullString `db:"default_namespace"`
	DefaultRole        sql.NullString `db:"default_role"`
	ExtAuthnDuo        sql.NullString `db:"ext_authn_duo"`
	ExtAuthnUID        sql.NullString `db:"ext_authn_uid"`
	MinsToBypassMFA    sql.NullString `db:"mins_to_bypass_mfa"`
	Owner              sql.NullString `db:"owner"`
	LastSuccessLogin   sql.NullString `db:"last_success_login"`
	ExpiresAtTime      sql.NullString `db:"expires_at_time"`
	LockedUntilTime    sql.NullString `db:"locked_until_time"`
	HasPassword        sql.NullString `db:"has_password"`
}

func (u User) Address() string {
	return fmt.Sprintf("%v.%v", SnowflakeUser, strings.ToLower(u.Name.String))
}
func (u User) ID() string {
	return u.Name.String
}

func (u User) HCL() []byte {
	return GenerateUser(u)
}

func GenerateUser(user User) []byte {
	f := hclwrite.NewEmptyFile()
	rootBody := f.Body()
	newBlock := rootBody.AppendNewBlock("resource", []string{"snowflake_user", strings.ToLower(user.Name.String)})
	tableBody := newBlock.Body()

	h := helper{File: f, Body: tableBody}
	h.SetAttributeNullString("name", user.Name).
		SetAttributeNullBool("disabled", user.Disabled).
		SetAttributeNullString("comment", user.Comment).
		SetAttributeNullString("login_name", user.LoginName).
		SetAttributeNullString("default_role", user.DefaultRole).
		SetAttributeNullString("default_namespace", user.DefaultNamespace).
		SetAttributeNullString("default_warehouse", user.DefaultWarehouse).
		SetAttributeNullString("email", user.Email).
		SetAttributeNullString("display_name", user.DisplayName).
		SetAttributeNullString("first_name", user.FirstName).
		SetAttributeNullString("last_name", user.LastName)

	return f.Bytes()
}

func ListUsers(db *sql.DB) ([]User, error) {
	stmt := "SHOW USERS"
	rows, err := Query(db, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := []User{}
	err = sqlx.StructScan(rows, &users)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no users found")
		return nil, nil
	}
	return users, errors.Wrapf(err, "unable to scan row for %s", stmt)
}
