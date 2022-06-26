/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"

	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateWarehousesCmd represents the generateWarehouses command
var generateWarehousesCmd = &cobra.Command{
	Use:   "generateWarehouses",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {

		fmt.Println("GenerateWarehouses commmand")
		bariloche.GenerateWareshouses()

	},
}

func init() {
	rootCmd.AddCommand(generateWarehousesCmd)
}
