/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/ezeql/bariloche/pkg/snowflake"
	"github.com/spf13/cobra"
)

// generateDatabasesCmd represents the generateDatabases command
var generateDatabasesCmd = &cobra.Command{
	Use:   "generateDatabases",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generateDatabases called")
		sdb, err := bariloche.GetDB()
		if err != nil {
			log.Fatal(err)
		}

		dbs, err := snowflake.ListDatabases(sdb)
		if err != nil {
			log.Fatalln(err)
		}

		outputDir := bariloche.DefaultDir()
		var res bariloche.TFResources

		for _, db := range dbs {
			res.Collect(db)
		}

		outFile := filepath.Join(outputDir, "database.tf")

		if err := bariloche.RunGenerateTerraformFiles(res, outputDir, outFile); err != nil {
			log.Fatalln(err)
		}

		if err := bariloche.RunTerraformImport(res, outputDir); err != nil {
			log.Fatalln(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(generateDatabasesCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// generateDatabasesCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// generateDatabasesCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
