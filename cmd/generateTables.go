/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"

	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateTablesCmd represents the generateTables command
var generateTablesCmd = &cobra.Command{
	Use:   "generateTables",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generateTables called")

		databaseName := cmd.Flag("databaseName").Value.String()
		schemaName := cmd.Flag("schemaName").Value.String()

		bariloche.GenerateTables(databaseName, schemaName)
	},
}

func init() {
	rootCmd.AddCommand(generateTablesCmd)

	generateTablesCmd.PersistentFlags().StringVar(&databaseName, "databaseName", "", "database name")
	generateTablesCmd.PersistentFlags().StringVar(&schemaNema, "schemaName", "", "schema name")
}
