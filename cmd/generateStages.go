/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"

	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateStagesCmd represents the generateStages command
var generateStagesCmd = &cobra.Command{
	Use:   "generateStages",
	Short: "Generates terraform files representing Snowflake Stages",
	Long:  `Generates terraform files representing Snowflake Stages`,
	Run: func(cmd *cobra.Command, args []string) {

		fmt.Printf("args: %v\n", args)
		dbName := cmd.Flag("tableName").Value.String()
		schemaName := cmd.Flag("schemaName").Value.String()

		fmt.Printf("dbName: %v\n", dbName)
		fmt.Printf("schemaNema: %v\n", schemaNema)

		bariloche.GenerateStages(dbName, schemaName)
	},
}

func init() {
	rootCmd.AddCommand(generateStagesCmd)

	generateStagesCmd.PersistentFlags().StringVar(&tableName, "tableName", "", "table name")

}
