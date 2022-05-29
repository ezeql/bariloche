package snowflake

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

const SnowflakeTable = "snowflake_table"

type TableB struct {
	CreatedOn           sql.NullString `db:"created_on"`
	Name                sql.NullString `db:"name"`
	DatabaseName        sql.NullString `db:"database_name"`
	SchemaName          sql.NullString `db:"schema_name"`
	Kind                sql.NullString `db:"kind"`
	Comment             sql.NullString `db:"comment"`
	ClusterBy           sql.NullString `db:"cluster_by"`
	Rows                sql.NullString `db:"row"`
	Bytes               sql.NullString `db:"bytes"`
	Owner               sql.NullString `db:"owner"`
	RetentionTime       sql.NullString `db:"retention_time"`
	AutomaticClustering sql.NullString `db:"automatic_clustering"`
	ChangeTracking      sql.NullString `db:"change_tracking"`

	Columns Columns `db:"-"`
}

func (t TableB) Address() string {
	return fmt.Sprintf("%v.%v", SnowflakeTable, strings.ToLower(t.Name.String))
}
func (t TableB) ID() string {
	return TableID(t)
}

func (t TableB) HCL() []byte {
	return GenerateTable(t)
}

func TableID(t TableB) string {
	return fmt.Sprintf("%v|%v|%v", t.DatabaseName.String, t.SchemaName.String, t.Name.String)
}

func TableFilePath(t TableB) string {
	return fmt.Sprintf("table_%v.tf", strings.ToLower(t.Name.String))
}

func ListTables(databaseName string, schemaName string, db *sql.DB) ([]TableB, error) {
	stmt := fmt.Sprintf(`SHOW TABLES IN SCHEMA "%s"."%v"`, databaseName, schemaName)
	rows, err := Query(db, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := []TableB{}
	err = sqlx.StructScan(rows, &tables)
	if err == sql.ErrNoRows {
		log.Printf("[DEBUG] no tables found")
		return nil, nil
	}
	// return tables, errors.Wrapf(err, "unable to scan row for %s", stmt)

	for i := range tables {

		c, err := ListColumns(tables[i], db)
		if err != nil {
			return nil, err
		}
		tables[i].Columns = c
	}

	return tables, nil

}

func GenerateTable(table TableB) []byte {
	clusterBy := ClusterStatementToList(table.ClusterBy.String)

	h := buildTerraformHelper(SnowflakeTable, table.Name.String).
		SetAttributeString("name", table.Name.String).
		SetAttributeString("schema", table.SchemaName.String).
		SetAttributeString("database", table.DatabaseName.String).
		SetAttributeList("cluster_by", clusterBy).
		SetAttributeString("comment", table.Comment.String).
		LineBreak()

	for _, c := range table.Columns {
		h.SetTableColumn(c)
	}

	// for _, c := range f {
	// 	v, ok := c.(map[string]interface{})
	// 	if !ok {
	// 		fmt.Println("cannot cast to map string")
	// 		continue
	// 	}

	// 	var columnName string
	// 	var columnType string
	// 	var columnComment string
	// 	var columnNullable bool
	// 	var columnDefault string

	// 	for k, s := range v {
	// 		switch k {
	// 		case "name":
	// 			columnName = fmt.Sprintf("%s", s)
	// 		case "type":
	// 			columnType = fmt.Sprintf("%s", s)
	// 		case "comment":
	// 			columnComment = fmt.Sprintf("%s", s)
	// 		case "nullable":
	// 			columnNullable = s.(bool)
	// 		case "default":

	// 			d := s.([]interface{})
	// 			fmt.Printf("d: %v\n", d)

	// 			dd := d[0].(map[string]interface{})

	// 			fmt.Printf("dd: %v\n", dd)

	// 			for _, v := range dd {
	// 				fmt.Printf("v: %v\n", v)
	// 			}

	// 			// def := map[string]interface{}{}
	// 			// switch col._default._type {
	// 			// case columnDefaultTypeConstant:
	// 			// 	def["constant"] = col._default.UnescapeConstantSnowflakeString(col._type)
	// 			// case columnDefaultTypeExpression:
	// 			// 	def["expression"] = col._default.expression
	// 			// case columnDefaultTypeSequence:
	// 			// 	def["sequence"] = col._default.expression
	// 			// }

	// 			// flat["default"] = []interface{}{def}

	// 		}

	// 	}

	// 	h.SetTableColumn(columnName, columnType, columnComment, columnDefault, columnNullable)
	// }

	return h.File.Bytes()
}

func ListColumns(table TableB, db *sql.DB) (Columns, error) {
	builder := Table(table.Name.String, table.DatabaseName.String, table.SchemaName.String)

	// row := snowflake.QueryRow(db, builder.Show())
	// _, err := snowflake.ScanTable(row)
	// if err == sql.ErrNoRows {
	// 	return nil, err
	// }
	// if err != nil {
	// 	return nil, err
	// }

	// Describe the table to read the cols
	tableDescriptionRows, err := Query(db, builder.ShowColumns())
	if err != nil {
		// return err
		return nil, err
	}

	tableDescription, err := ScanTableDescription(tableDescriptionRows)
	if err != nil {
		// return err
		return nil, err
	}

	// showPkrows, err := snowflake.Query(sdb.DB, builder.ShowPrimaryKeys())
	// if err != nil {
	// 	// return err
	// 	return
	// }

	// pkDescription, err := snowflake.ScanPrimaryKeyDescription(showPkrows)
	// if err != nil {
	// 	// return err
	// 	return
	// }

	// "column":              snowflake.NewColumns(tableDescription).Flatten(),
	// "cluster_by":          snowflake.ClusterStatementToList(table.ClusterBy.String),
	// "primary_key":         snowflake.FlattenTablePrimaryKey(pkDescription),

	// columns := snowflake.NewColumns(tableDescription)

	// log.Println(columns.Flatten())
	// table.
	// log.Println(table.SchemaName.String)
	// log.Println(table.DatabaseName.String)
	// log.Println(table.ClusterBy.String)

	// log.Println(table.Rows)

	// out := generateTable(
	// 	table.DatabaseName.String,
	// 	table.SchemaName.String,
	// 	table.TableName.String,
	// 	table.ClusterBy.String,
	// 	snowflake.NewColumns(tableDescription),
	// )

	cols := NewColumns(tableDescription)

	return cols, nil

}

// PrimaryKey structure that represents a tables primary key
type PrimaryKey struct {
	name string
	keys []string
}

// WithName set the primary key name
func (pk *PrimaryKey) WithName(name string) *PrimaryKey {
	pk.name = name
	return pk
}

// WithKeys set the primary key keys
func (pk *PrimaryKey) WithKeys(keys []string) *PrimaryKey {
	pk.keys = keys
	return pk
}

type ColumnDefaultType int

const (
	columnDefaultTypeConstant = iota
	columnDefaultTypeSequence
	columnDefaultTypeExpression
)

type ColumnDefault struct {
	_type      ColumnDefaultType
	expression string
}

func NewColumnDefaultWithConstant(constant string) *ColumnDefault {
	return &ColumnDefault{
		_type:      columnDefaultTypeConstant,
		expression: constant,
	}
}

func NewColumnDefaultWithExpression(expression string) *ColumnDefault {
	return &ColumnDefault{
		_type:      columnDefaultTypeExpression,
		expression: expression,
	}
}

func NewColumnDefaultWithSequence(sequence string) *ColumnDefault {
	return &ColumnDefault{
		_type:      columnDefaultTypeSequence,
		expression: sequence,
	}
}

func (d *ColumnDefault) String(columnType string) string {
	columnType = strings.ToUpper(columnType)

	switch {
	case d._type == columnDefaultTypeExpression:
		return d.expression

	case d._type == columnDefaultTypeSequence:
		return fmt.Sprintf(`%v.NEXTVAL`, d.expression)

	case d._type == columnDefaultTypeConstant && (strings.Contains(columnType, "CHAR") || columnType == "STRING" || columnType == "TEXT"):
		return EscapeSnowflakeString(d.expression)

	default:
		return d.expression
	}
}

func (d *ColumnDefault) UnescapeConstantSnowflakeString(columnType string) string {
	columnType = strings.ToUpper(columnType)

	if d._type == columnDefaultTypeConstant && (strings.Contains(columnType, "CHAR") || columnType == "STRING" || columnType == "TEXT") {
		return UnescapeSnowflakeString(d.expression)
	}

	return d.expression
}

// Column structure that represents a table column
type Column struct {
	name     string
	_type    string // type is reserved
	nullable bool
	_default *ColumnDefault // default is reserved
	comment  string         // pointer as value is nullable
}

func FlattenTablePrimaryKey(pkds []primaryKeyDescription) []interface{} {
	flattened := []interface{}{}
	if len(pkds) == 0 {
		return flattened
	}

	sort.SliceStable(pkds, func(i, j int) bool {
		num1, _ := strconv.Atoi(pkds[i].KeySequence.String)
		num2, _ := strconv.Atoi(pkds[j].KeySequence.String)
		return num1 < num2
	})
	//sort our keys on the key sequence

	flat := map[string]interface{}{}
	var keys []string
	var name string
	var nameSet bool

	for _, pk := range pkds {
		//set as empty string, sys_constraint means it was an unnnamed constraint
		if strings.Contains(pk.ConstraintName.String, "SYS_CONSTRAINT") && !nameSet {
			name = ""
			nameSet = true
		}
		if !nameSet {
			name = pk.ConstraintName.String
			nameSet = true
		}

		keys = append(keys, pk.ColumnName.String)

	}

	flat["name"] = name
	flat["keys"] = keys
	flattened = append(flattened, flat)
	return flattened

}

type Columns []Column

// NewColumns generates columns from a table description
func NewColumns(tds []tableDescription) Columns {
	cs := []Column{}
	for _, td := range tds {
		if td.Kind.String != "COLUMN" {
			continue
		}

		cs = append(cs, Column{
			name:     td.Name.String,
			_type:    td.Type.String,
			nullable: td.IsNullable(),
			_default: td.ColumnDefault(),
			comment:  td.Comment.String,
		})
	}
	return Columns(cs)
}

func (c Columns) Flatten() []interface{} {
	flattened := []interface{}{}
	for _, col := range c {
		flat := map[string]interface{}{}
		flat["name"] = col.name
		flat["type"] = col._type
		flat["nullable"] = col.nullable
		flat["comment"] = col.comment

		if col._default != nil {
			def := map[string]interface{}{}
			switch col._default._type {
			case columnDefaultTypeConstant:
				def["constant"] = col._default.UnescapeConstantSnowflakeString(col._type)
			case columnDefaultTypeExpression:
				def["expression"] = col._default.expression
			case columnDefaultTypeSequence:
				def["sequence"] = col._default.expression
			}

			flat["default"] = []interface{}{def}
		}

		flattened = append(flattened, flat)
	}
	return flattened
}

// TableBuilder abstracts the creation of SQL queries for a Snowflake schema
type TableBuilder struct {
	name    string
	db      string
	schema  string
	columns Columns
	// comment   string
	clusterBy []string
	// primaryKey              PrimaryKey
	// dataRetentionTimeInDays int
	// changeTracking          bool
	// defaultDDLCollation     string
}

// QualifiedName prepends the db and schema if set and escapes everything nicely
func (tb *TableBuilder) QualifiedName() string {
	var n strings.Builder

	if tb.db != "" && tb.schema != "" {
		n.WriteString(fmt.Sprintf(`"%v"."%v".`, tb.db, tb.schema))
	}

	if tb.db != "" && tb.schema == "" {
		n.WriteString(fmt.Sprintf(`"%v"..`, tb.db))
	}

	if tb.db == "" && tb.schema != "" {
		n.WriteString(fmt.Sprintf(`"%v".`, tb.schema))
	}

	n.WriteString(fmt.Sprintf(`"%v"`, tb.name))

	return n.String()
}

//Function to get clustering definition
func (tb *TableBuilder) GetClusterKeyString() string {

	return JoinStringList(tb.clusterBy[:], ", ")
}

func JoinStringList(instrings []string, delimiter string) string {

	return fmt.Sprint(strings.Join(instrings[:], delimiter))

}

// func quoteStringList(instrings []string) []string {
// 	var clean []string
// 	for _, word := range instrings {
// 		quoted := fmt.Sprintf(`"%s"`, word)
// 		clean = append(clean, quoted)

// 	}
// 	return clean

// }

//function to take the literal snowflake cluster statement returned from SHOW TABLES and convert it to a list of keys.
func ClusterStatementToList(clusterStatement string) []string {
	if clusterStatement == "" {
		return nil
	}

	cleanStatement := strings.TrimSuffix(strings.Replace(clusterStatement, "LINEAR(", "", 1), ")")
	// remove cluster statement and trailing parenthesis

	var clean []string

	for _, s := range strings.Split(cleanStatement, ",") {
		clean = append(clean, strings.TrimSpace(s))
	}

	return clean

}

// Table returns a pointer to a Builder that abstracts the DDL operations for a table.
//
// Supported DDL operations are:
//   - ALTER TABLE
//   - DROP TABLE
//   - SHOW TABLES
//
// [Snowflake Reference](https://docs.snowflake.com/en/sql-reference/ddl-table.html)
func Table(name, db, schema string) *TableBuilder {
	return &TableBuilder{
		name:   name,
		db:     db,
		schema: schema,
	}
}

// Table returns a pointer to a Builder that abstracts the DDL operations for a table.
//
// Supported DDL operations are:
//   - CREATE TABLE
//
// [Snowflake Reference](https://docs.snowflake.com/en/sql-reference/ddl-table.html)
func TableWithColumnDefinitions(name, db, schema string, columns Columns) *TableBuilder {
	return &TableBuilder{
		name:    name,
		db:      db,
		schema:  schema,
		columns: columns,
	}
}

// Show returns the SQL query that will show a table.
func (tb *TableBuilder) Show() string {
	return fmt.Sprintf(`SHOW TABLES LIKE '%v' IN SCHEMA "%v"."%v"`, tb.name, tb.db, tb.schema)
}

func (tb *TableBuilder) ShowColumns() string {
	return fmt.Sprintf(`DESC TABLE %s`, tb.QualifiedName())
}

func (tb *TableBuilder) ShowPrimaryKeys() string {
	return fmt.Sprintf(`SHOW PRIMARY KEYS IN TABLE %s`, tb.QualifiedName())
}

type table struct {
	CreatedOn           sql.NullString `db:"created_on"`
	TableName           sql.NullString `db:"name"`
	DatabaseName        sql.NullString `db:"database_name"`
	SchemaName          sql.NullString `db:"schema_name"`
	Kind                sql.NullString `db:"kind"`
	Comment             sql.NullString `db:"comment"`
	ClusterBy           sql.NullString `db:"cluster_by"`
	Rows                sql.NullString `db:"row"`
	Bytes               sql.NullString `db:"bytes"`
	Owner               sql.NullString `db:"owner"`
	RetentionTime       sql.NullInt32  `db:"retention_time"`
	AutomaticClustering sql.NullString `db:"automatic_clustering"`
	ChangeTracking      sql.NullString `db:"change_tracking"`
}

func ScanTable(row *sqlx.Row) (*table, error) {
	t := &table{}
	e := row.StructScan(t)
	return t, e
}

type tableDescription struct {
	Name     sql.NullString `db:"name"`
	Type     sql.NullString `db:"type"`
	Kind     sql.NullString `db:"kind"`
	Nullable sql.NullString `db:"null?"`
	Default  sql.NullString `db:"default"`
	Comment  sql.NullString `db:"comment"`
}

func (td *tableDescription) IsNullable() bool {
	if td.Nullable.String == "Y" {
		return true
	} else {
		return false
	}
}

func (td *tableDescription) ColumnDefault() *ColumnDefault {
	if !td.Default.Valid {
		return nil
	}

	if strings.HasSuffix(td.Default.String, ".NEXTVAL") {
		return NewColumnDefaultWithSequence(strings.TrimSuffix(td.Default.String, ".NEXTVAL"))
	}

	if strings.Contains(td.Default.String, "(") && strings.Contains(td.Default.String, ")") {
		return NewColumnDefaultWithExpression(td.Default.String)
	}

	if strings.Contains(td.Type.String, "CHAR") || td.Type.String == "STRING" || td.Type.String == "TEXT" {
		return NewColumnDefaultWithConstant(UnescapeSnowflakeString(td.Default.String))
	}

	return NewColumnDefaultWithConstant(td.Default.String)
}

type primaryKeyDescription struct {
	ColumnName     sql.NullString `db:"column_name"`
	KeySequence    sql.NullString `db:"key_sequence"`
	ConstraintName sql.NullString `db:"constraint_name"`
}

func ScanTableDescription(rows *sqlx.Rows) ([]tableDescription, error) {
	tds := []tableDescription{}
	for rows.Next() {
		td := tableDescription{}
		err := rows.StructScan(&td)
		if err != nil {
			return nil, err
		}
		tds = append(tds, td)
	}
	return tds, rows.Err()
}

func ScanPrimaryKeyDescription(rows *sqlx.Rows) ([]primaryKeyDescription, error) {
	pkds := []primaryKeyDescription{}
	for rows.Next() {
		pk := primaryKeyDescription{}
		err := rows.StructScan(&pk)
		if err != nil {
			return nil, err
		}
		pkds = append(pkds, pk)
	}
	return pkds, rows.Err()
}
