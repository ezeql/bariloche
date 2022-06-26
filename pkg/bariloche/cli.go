package bariloche

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/ezeql/bariloche/pkg/snowflake"
	"github.com/hashicorp/terraform-exec/tfexec"
	"github.com/jmoiron/sqlx"
	sf "github.com/snowflakedb/gosnowflake"
	"github.com/spf13/viper"
)

type TFResources struct {
	Data []snowflake.TFResource
}

func (r *TFResources) Collect(res snowflake.TFResource) {
	r.Data = append(r.Data, res)
}

func RunGenerateTerraformFiles(resources *TFResources, outputDir string, outFile string) error {
	log.Println("generating terraform files")
	if len(resources.Data) == 0 {
		return nil
	}
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return err
	}

	if err := snowflake.GenerateProvider(outputDir); err != nil {
		return err
	}

	fileAbs := strings.ToLower(filepath.Join(outputDir, outFile))

	f, err := os.Create(fileAbs)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer func() error { //check defer return
		return w.Flush()
	}()

	for _, r := range resources.Data {
		w.Write(r.HCL())
	}

	return nil
}

func RunTerraformImport(resources TFResources, outputDir string) error {
	log.Println("importing state")

	//TODO:  ====================== CHECK below text for terraform path dowloading vs reusing =======================

	// tmpDir, err := ioutil.TempDir("", "tfinstall")
	// if err != nil {
	// 	return fmt.Errorf("error creating temp dir: %w", err)
	// }
	// defer os.RemoveAll(tmpDir)

	// log.Println("finding latest terraform version")
	// execPath, err := tfinstall.Find(context.Background(), tfinstall.LatestVersion(outputDir, false))
	// if err != nil {
	// 	return fmt.Errorf("error locating Terraform binary: %w", err)
	// }

	//TODO:  ====================== CHECK the above =======================

	log.Println("running new terraform")
	tf, err := tfexec.NewTerraform(outputDir, "/opt/homebrew/bin/terraform")
	if err != nil {
		return fmt.Errorf("error running NewTerraform: %w", err)
	}

	log.Println("running init")
	err = tf.Init(context.Background(), tfexec.Upgrade(false))
	if err != nil {
		return fmt.Errorf("error running Init: %w", err)
	}

	for _, res := range resources.Data {
		err = tf.Import(context.Background(), res.Address(), res.ID())
		log.Printf("Import: Address: '%v' ID: '%v'", res.Address(), res.ID())
		if err != nil {
			log.Fatalf("Error while importing: %v \n", err)
		}

		tfimp := snowflake.GenerateTFImport(res)
		log.Println(tfimp)
	}

	if _, err := tf.Show(context.Background()); err != nil {
		return fmt.Errorf("error running Show: %w", err)
	}

	return nil
}

func DefaultDir() string {
	return "generated"
}

func GetDB() (*sqlx.DB, error) {
	account := viper.GetString("ACCOUNT")
	user := viper.GetString("USER")
	password := viper.GetString("PASSWORD")
	role := viper.GetString("ROLE")
	port := 443

	cfg := &sf.Config{
		Account:  account,
		User:     user,
		Password: password,
		Port:     port,
		Role:     role,
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		return nil, err
	}

	return sqlx.Open("snowflake", dsn)
}

func GenerateTables(databaseName, schemaName string) (*TFResources, error) {
	f := func(in *TFResources, sdb *sqlx.DB) error {
		tables, err := snowflake.ListTables(databaseName, schemaName, sdb.DB)
		if err != nil {
			return err
		}
		for _, table := range tables {
			if table.Name.String != "information_schema" {
				in.Collect(table)
			}

		}
		return nil
	}

	return generateResource(DefaultDir(), databaseName+"_"+schemaName+"_"+"table.tf", f)
}

func GenerateDatabases() (*TFResources, error) {
	bannedDatabases := []string{"snowflake"}
	f := func(in *TFResources, sdb *sqlx.DB) error {
		dbs, err := snowflake.ListDatabases(sdb)
		if err != nil {
			return err
		}

		for _, u := range dbs {
			if !isBanned(u.DBName.String, bannedDatabases) &&
				(!strings.HasPrefix(strings.ToLower(u.DBName.String), "snowflake_sample_data")) {
				in.Collect(u)

			}
		}

		return nil
	}

	return generateResource(DefaultDir(), "database.tf", f)

}
func GenerateStages(databaseName, schemaName string) (*TFResources, error) {
	f := func(in *TFResources, sdb *sqlx.DB) error {
		dbs, err := snowflake.ListStages(databaseName, schemaName, sdb.DB)
		if err != nil {
			return err
		}
		for _, u := range dbs {
			in.Collect(u)
		}
		return nil
	}

	return generateResource(DefaultDir(), databaseName+"_"+schemaName+"_"+"stage.tf", f)
}

func GeneratePipes(databaseName, schemaName string) (*TFResources, error) {
	f := func(in *TFResources, sdb *sqlx.DB) error {
		dbs, err := snowflake.ListPipes(databaseName, schemaName, sdb.DB)
		if err != nil {
			return err
		}
		for _, u := range dbs {
			in.Collect(u)
		}
		return nil
	}

	return generateResource(DefaultDir(), databaseName+"_"+schemaName+"_"+"pipe.tf", f)
}

func GenerateUsers() (*TFResources, error) {
	bannedUsers := []string{"snowflake"}
	f := func(in *TFResources, sdb *sqlx.DB) error {
		users, err := snowflake.ListUsers(sdb.DB)
		if err != nil {
			return err
		}
		for _, user := range users {
			if !isBanned(strings.ToLower(user.Name.String), bannedUsers) {
				in.Collect(user)
			}

		}
		return nil
	}

	return generateResource(DefaultDir(), "user.tf", f)
}

func GenerateSchema(databaseName string) (*TFResources, error) {
	bannedSchema := []string{"information_schema"}
	f := func(in *TFResources, sdb *sqlx.DB) error {
		schemas, err := snowflake.ListSchemas(databaseName, sdb.DB)
		if err != nil {
			return err
		}
		for _, s := range schemas {

			if !isBanned(strings.ToLower(s.Name.String), bannedSchema) {
				in.Collect(s)
			}

		}
		return nil
	}

	return generateResource(DefaultDir(), databaseName+"_"+"schema.tf", f)

}

func isBanned(name string, list []string) bool {

	for _, key := range list {
		if strings.EqualFold(key, name) {
			return true
		}
	}
	return false
}
func GenerateViews(databaseName, schemaName string) (*TFResources, error) {
	f := func(in *TFResources, sdb *sqlx.DB) error {
		views, err := snowflake.ListViews(databaseName, schemaName, sdb.DB)
		if err != nil {
			return err
		}
		for _, v := range views {
			in.Collect(v)
		}
		return nil
	}

	return generateResource(DefaultDir(), databaseName+"_"+schemaName+"_"+"view.tf", f)
}

func GenerateRoles() (*TFResources, error) {
	f := func(in *TFResources, sdb *sqlx.DB) error {
		roles, err := snowflake.ListRoles(sdb.DB)
		if err != nil {
			return err
		}
		for _, r := range roles {
			in.Collect(r)
		}
		return nil
	}

	return generateResource(DefaultDir(), "role.tf", f)
}

func GenerateWareshouses() (*TFResources, error) {
	f := func(in *TFResources, sdb *sqlx.DB) error {
		warehouses, err := snowflake.ListWarehouses(sdb.DB)
		if err != nil {
			return err
		}
		for _, w := range warehouses {
			in.Collect(w)
		}
		return nil
	}

	return generateResource(DefaultDir(), "warehouse.tf", f)
}

type resfunc func(in *TFResources, sdb *sqlx.DB) error

func generateResource(outputDir string, outFile string, f resfunc) (*TFResources, error) {
	var res TFResources

	sdb, err := GetDB()
	if err != nil {
		return nil, err
	}

	if err := f(&res, sdb); err != nil {
		return nil, err
	}

	if err := RunGenerateTerraformFiles(&res, outputDir, outFile); err != nil {
		return nil, err
	}

	if err := RunTerraformImport(res, outputDir); err != nil {
		return nil, err
	}

	return &res, nil
}
