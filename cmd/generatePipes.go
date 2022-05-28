/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// generatePipesCmd represents the generatePipes command
var generatePipesCmd = &cobra.Command{
	Use:   "generatePipes",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generatePipes called")

		// databaseName := cmd.Flag("databaseName").Value.String()
		// schemaName := cmd.Flag("schemaName").Value.String()

		// bariloche.GeneratePipes()
	},
}

func init() {
	rootCmd.AddCommand(generatePipesCmd)

	// generatePipesCmd.PersistentFlags().StringVar(&databaseName, "databaseName", "", "database name")
	// generatePipesCmd.PersistentFlags().StringVar(&schemaNema, "schemaName", "", "schema name")
}
