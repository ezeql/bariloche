/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateStagesCmd represents the generateStages command
var generateStagesCmd = &cobra.Command{
	Use:   "generateStages",
	Short: "Generates terraform files representing Snowflake Stages",
	Long:  `Generates terraform files representing Snowflake Stages`,
	Run: func(cmd *cobra.Command, args []string) {
		dbName := cmd.Flag("databaseName").Value.String()
		schemaName := cmd.Flag("schemaName").Value.String()
		bariloche.GenerateStages(dbName, schemaName)
	},
}

func init() {
	rootCmd.AddCommand(generateStagesCmd)
	generateStagesCmd.PersistentFlags().StringVar(&databaseName, "databaseName", "", "database name")
	generateStagesCmd.PersistentFlags().StringVar(&schemaNema, "schemaName", "", "schema name")
}
