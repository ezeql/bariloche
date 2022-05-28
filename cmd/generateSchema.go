/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"

	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateSchemaCmd represents the generateSchema command
var generateSchemaCmd = &cobra.Command{
	Use:   "generateSchema",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generateSchema called")
		dbName := cmd.Flag("databaseName").Value.String()
		bariloche.GenerateSchema(dbName)
	},
}

func init() {
	rootCmd.AddCommand(generateSchemaCmd)
}
