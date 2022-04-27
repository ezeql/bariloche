/*
Copyright © 2022 Ezequiel Moreno
*/

package cmd

import (
	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateUsersCmd represents the generateUsers command
var generateUsersCmd = &cobra.Command{
	Use:   "generateUsers",
	Short: "Generates terraform files representing Snowflake users",
	Long:  `Generates terraform files representing Snowflake users`,
	Run: func(cmd *cobra.Command, args []string) {
		bariloche.GenerateUsers()
	},
}

func init() {
	rootCmd.AddCommand(generateUsersCmd)
}
