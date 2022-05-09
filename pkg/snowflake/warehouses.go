package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const SnowflakeWarehouse = "snowflake_warehouse"

type Warehouse struct {
	Name            string    `db:"name"`
	State           string    `db:"state"`
	Type            string    `db:"type"`
	Size            string    `db:"size"`
	MinClusterCount int64     `db:"min_cluster_count"`
	MaxClusterCount int64     `db:"max_cluster_count"`
	StartedClusters int64     `db:"started_clusters"`
	Running         int64     `db:"running"`
	Queued          int64     `db:"queued"`
	IsDefault       string    `db:"is_default"`
	IsCurrent       string    `db:"is_current"`
	AutoSuspend     int64     `db:"auto_suspend"`
	AutoResume      bool      `db:"auto_resume"`
	Available       string    `db:"available"`
	Provisioning    string    `db:"provisioning"`
	Quiescing       string    `db:"quiescing"`
	Other           string    `db:"other"`
	CreatedOn       time.Time `db:"created_on"`
	ResumedOn       time.Time `db:"resumed_on"`
	UpdatedOn       time.Time `db:"updated_on"`
	Owner           string    `db:"owner"`
	Comment         string    `db:"comment"`
	ResourceMonitor string    `db:"resource_monitor"`
	Actives         int64     `db:"actives"`
	Pendings        int64     `db:"pendings"`
	Failed          int64     `db:"failed"`
	Suspended       int64     `db:"suspended"`
	UUID            string    `db:"uuid"`
	ScalingPolicy   string    `db:"scaling_policy"`
}

func (w Warehouse) Address() string {
	return fmt.Sprintf("%v.%v", SnowflakeWarehouse, strings.ToLower(w.Name))
}
func (w Warehouse) ID() string {
	return w.Name
}

// // Show returns the SQL query that will show the row representing this Warehouse.
// func (vb *WarehouseBuilder) Show() string {
// 	return fmt.Sprintf(`SHOW WarehouseS LIKE '%v' IN SCHEMA "%v"."%v"`, vb.name, vb.db, vb.schema)
// }

func (w Warehouse) HCL() []byte {
	panic("not implemented")
}

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
