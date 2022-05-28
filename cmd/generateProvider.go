/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// generateProviderCmd represents the generateProvider command
var generateProviderCmd = &cobra.Command{
	Use:   "generateProvider",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generateProvider called")
	},
}

func init() {
	rootCmd.AddCommand(generateProviderCmd)
}
