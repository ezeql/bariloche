/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"

	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateViewsCmd represents the generateViews command
var generateViewsCmd = &cobra.Command{
	Use:   "generateViews",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generateViews called")
		bariloche.GenerateViews("", "")
	},
}

func init() {
	rootCmd.AddCommand(generateViewsCmd)

	// generateViewsCmd.PersistentFlags().StringVar(&databaseName, "databaseName", "", "database name")
	// generateViewsCmd.PersistentFlags().StringVar(&schemaNema, "schemaName", "", "schema name")

}
