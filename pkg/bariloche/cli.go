package bariloche

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/ezeql/bariloche/pkg/snowflake"
	"github.com/hashicorp/terraform-exec/tfexec"
	"github.com/hashicorp/terraform-exec/tfinstall"
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

func RunTerraformImport(resources TFResources, outputDir string) {
	tmpDir, err := ioutil.TempDir("", "tfinstall")
	if err != nil {
		log.Fatalf("error creating temp dir: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	execPath, err := tfinstall.Find(context.Background(), tfinstall.LatestVersion(tmpDir, false))
	if err != nil {
		log.Fatalf("error locating Terraform binary: %s", err)
	}

	tf, err := tfexec.NewTerraform(outputDir, execPath)
	if err != nil {
		log.Fatalf("error running NewTerraform: %s", err)
	}

	err = tf.Init(context.Background(), tfexec.Upgrade(true))
	if err != nil {
		log.Fatalf("error running Init: %s", err)
	}

	for _, res := range resources.data {
		err = tf.Import(context.Background(), res.Address(), res.ID())
		if err != nil {
			// fmt.Printf("error running Import res: %s \n", err)
			continue
		}
	}

	state, err := tf.Show(context.Background())
	if err != nil {
		log.Fatalf("error running Show: %s", err)
	}

	fmt.Println("state", state.Values)
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

func GenerateStages(db, schema string) {
	sdb, err := GetDB()
	if err != nil {
		log.Fatal(err)
	}

	outputDir := DefaultDir()
	var res TFResources

	stages, err := snowflake.ListStages(db, schema, sdb.DB)
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
