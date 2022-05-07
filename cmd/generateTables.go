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
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generateTables called")

		databaseName := cmd.Flag("databaseName").Value.String()
		schemaName := cmd.Flag("schemaName").Value.String()

		fmt.Printf("databaseName: %v\n", databaseName)
		fmt.Printf("schemaNema: %v\n", schemaNema)

		bariloche.GenerateTables(databaseName, schemaName)
	},
}

func init() {
	rootCmd.AddCommand(generateTablesCmd)

	generateTablesCmd.PersistentFlags().StringVar(&databaseName, "databaseName", "", "database name")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// generateTablesCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// generateTablesCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
