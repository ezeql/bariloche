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
	data []snowflake.TFResource
}

func (r *TFResources) Collect(res snowflake.TFResource) {
	r.data = append(r.data, res)
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

	for _, r := range resources.data {
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

	for _, res := range resources.data {
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

func GenerateDatabases() {
	// snowflake.ListDatases()
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
