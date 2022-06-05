package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const SnowflakeWarehouse = "snowflake_warehouse"

type Warehouse struct {
	Name        string
	AutoResume  bool
	AutoSuspend int
	Comment     string
	// WID                             string
	InitiallySuspended              bool
	MaxClusterCount                 int
	MaxConcurrencyLevel             int
	MinClusterCount                 int
	ResourceMonitor                 string
	ScalingPolicy                   string
	StatementQueuedTimeoutInSeconds int
	StatementTimeoutInSeconds       int
	WaitForProvisioning             bool
	Size                            string
}

func (w Warehouse) Address() string {
	return fmt.Sprintf("%v.%v", SnowflakeWarehouse, strings.ToLower(w.Name))
}
func (w Warehouse) ID() string {
	return w.Name
}
func (w Warehouse) ResourceName() string {
	return w.Name
}

func (w Warehouse) HCL() []byte {
	return buildTerraformHelper(SnowflakeWarehouse, w.Name).
		SetAttributeString("name", w.Name).
		SetAttributeString("comment", w.Comment).File.Bytes()
}

// // Show returns the SQL query that will show the row representing this Warehouse.
// func (vb *WarehouseBuilder) Show() string {
// 	return fmt.Sprintf(`SHOW WarehouseS LIKE '%v' IN SCHEMA "%v"."%v"`, vb.name, vb.db, vb.schema)
// }

func ListWarehouses(db *sql.DB) ([]Warehouse, error) {
	stmt := "SHOW WAREHOUSES"
	rows, err := Query(db, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbs := []Warehouse{}
	err = sqlx.StructScan(rows, &dbs)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no warehouses found")
		return nil, nil
	}
	return dbs, errors.Wrapf(err, "unable to scan row for %s", stmt)
}
