package snowflake

const SnowflakeFunction = "snowflake_function"

type Function struct {
	Database   string `db:"created_on"`
	Name       string `db:"name"`
	ReturnType string `db:"return_type"`
	Schema     string `db:"schema"`
	statement  string `db:"statement"`
}

// func (f Function) Address() string {
// 	// return fmt.Sprintf("%v.%v", SnowflakeFunction, strings.ToLower(f.Name))
// }
// func (f Function) ID() string {
// 	// return PipeID(f.Name)
// }
