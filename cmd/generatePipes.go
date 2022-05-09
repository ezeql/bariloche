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

// generatePipesCmd represents the generatePipes command
var generatePipesCmd = &cobra.Command{
	Use:   "generatePipes",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generatePipes called")

		databaseName := cmd.Flag("databaseName").Value.String()
		schemaName := cmd.Flag("schemaName").Value.String()

		sdb, err := bariloche.GetDB()
		if err != nil {
			log.Fatal(err)
		}

		ps, err := snowflake.ListPipes(databaseName, schemaName, sdb.DB)
		if err != nil {
			log.Fatalln(err)
		}

		outputDir := bariloche.DefaultDir()
		var res bariloche.TFResources

		for _, p := range ps {
			res.Collect(p)
		}

		outFile := filepath.Join(outputDir, "pipe.tf")

		if err := bariloche.RunGenerateTerraformFiles(res, outputDir, outFile); err != nil {
			log.Fatalln(err)
		}

		if err := bariloche.RunTerraformImport(res, outputDir); err != nil {
			log.Fatalln(err)
		}

	},
}

func init() {
	rootCmd.AddCommand(generatePipesCmd)

	generatePipesCmd.PersistentFlags().StringVar(&databaseName, "databaseName", "", "database name")
	generatePipesCmd.PersistentFlags().StringVar(&schemaNema, "schemaName", "", "schema name")
}
