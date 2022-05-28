package bariloche

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

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

func RunGenerateTerraformFiles(resources TFResources, outputDir string, outFile string) error {
	log.Println("generating terraform files")
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return err
	}

	if err := snowflake.GenerateProvider(outputDir); err != nil {
		return err
	}

	f, err := os.Create(outFile)
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
		if err != nil {
			fmt.Printf("error running Import res: %s \n", err)
			continue
		}
	}

	if _, err := tf.Show(context.Background()); err != nil {
		return fmt.Errorf("error running Show: %w", err)
	}
	// fmt.Println("state", state.Values)

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

func GenerateTables(dbName, schemaName string) {
	sdb, err := GetDB()
	if err != nil {
		log.Fatal(err)
	}

	outputDir := DefaultDir()
	var res TFResources

	tables, err := snowflake.ListTables(dbName, schemaName, sdb.DB)
	if err != nil {
		log.Fatal(err)
	}

	for _, t := range tables {
		res.Collect(t)
	}

	outFile := filepath.Join(outputDir, "table.tf")

	RunGenerateTerraformFiles(res, outputDir, outFile)
	RunTerraformImport(res, outputDir)

}

func GenerateDatabases() (*TFResources, error) {
	f := func(in TFResources, sdb *sqlx.DB) error {
		dbs, err := snowflake.ListDatabases(sdb)
		if err != nil {
			return err
		}
		for _, u := range dbs {
			in.Collect(u)
		}
		return nil
	}

	return generateResource(DefaultDir(), "database.tf", f)

}
func GenerateStages(dbName, schemaName string) {
	sdb, err := GetDB()
	if err != nil {
		log.Fatal(err)
	}

	outputDir := DefaultDir()
	var res TFResources

	stages, err := snowflake.ListStages(dbName, schemaName, sdb.DB)
	if err != nil {
		log.Fatal(err)
	}

	for _, s := range stages {
		res.Collect(s)
	}

	outFile := filepath.Join(outputDir, "stage.tf")

	RunGenerateTerraformFiles(res, outputDir, outFile)
	RunTerraformImport(res, outputDir)
}

func GeneratePipes(databaseName, schemaName string) {
	sdb, err := GetDB()
	if err != nil {
		log.Fatal(err)
	}

	ps, err := snowflake.ListPipes(databaseName, schemaName, sdb.DB)
	if err != nil {
		log.Fatalln(err)
	}

	outputDir := DefaultDir()
	var res TFResources

	for _, p := range ps {
		res.Collect(p)
	}

	outFile := filepath.Join(outputDir, "pipe.tf")

	if err := RunGenerateTerraformFiles(res, outputDir, outFile); err != nil {
		log.Fatalln(err)
	}

	if err := RunTerraformImport(res, outputDir); err != nil {
		log.Fatalln(err)
	}
}

func GenerateUsers() {
	sdb, err := GetDB()
	if err != nil {
		log.Fatal(err)
	}

	outputDir := DefaultDir()
	var res TFResources

	users, err := snowflake.ListUsers(sdb.DB)
	if err != nil {
		log.Fatal(err)
	}

	for _, u := range users {
		res.Collect(u)
	}

	outFile := filepath.Join(outputDir, "user.tf")

	RunGenerateTerraformFiles(res, outputDir, outFile)
	RunTerraformImport(res, outputDir)
}

func GenerateSchema(databaseName string) (*TFResources, error) {
	f := func(in TFResources, sdb *sqlx.DB) error {
		users, err := snowflake.ListSchemas(databaseName, sdb.DB)
		if err != nil {
			return err
		}
		for _, u := range users {
			in.Collect(u)
		}
		return nil
	}

	return generateResource(DefaultDir(), "schema.tf", f)

}

func GenerateViews(databaseName, schemaName string) (*TFResources, error) {
	f := func(in TFResources, sdb *sqlx.DB) error {
		views, err := snowflake.ListViews(databaseName, schemaName, sdb.DB)
		if err != nil {
			return err
		}
		for _, v := range views {
			in.Collect(v)
		}
		return nil
	}

	return generateResource(DefaultDir(), "view.tf", f)
}

func GenerateRoles() (*TFResources, error) {
	f := func(in TFResources, sdb *sqlx.DB) error {
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
	f := func(in TFResources, sdb *sqlx.DB) error {
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

type resfunc func(in TFResources, sdb *sqlx.DB) error

func generateResource(outputDir string, outFile string, f resfunc) (*TFResources, error) {
	sdb, err := GetDB()
	if err != nil {
		return nil, err
	}

	var res TFResources

	err = f(res, sdb)

	if err != nil {
		return nil, err
	}

	RunGenerateTerraformFiles(res, outputDir, outFile)
	RunTerraformImport(res, outputDir)

	return &res, nil
}
