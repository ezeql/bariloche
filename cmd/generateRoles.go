/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"

	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateRolesCmd represents the generateRoles command
var generateRolesCmd = &cobra.Command{
	Use:   "generateRoles",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generateRoles called")
		bariloche.GenerateRoles()
	},
}

func init() {
	rootCmd.AddCommand(generateRolesCmd)
}
