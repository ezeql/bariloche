/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"

	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateDatabasesCmd represents the generateDatabases command
var generateDatabasesCmd = &cobra.Command{
	Use:   "generateDatabases",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generateDatabases called")
		bariloche.GenerateDatabases()
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
