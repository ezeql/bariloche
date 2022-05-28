/*
Copyright © 2022 Ezequiel Moreno
*/
package cmd

import (
	"fmt"
	"log"

	"github.com/ezeql/bariloche/pkg/bariloche"
	"github.com/spf13/cobra"
)

// generateAllCmd represents the generateAll command
var generateAllCmd = &cobra.Command{
	Use:   "generateAll",
	Short: "",
	Long:  "",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("generateAll called")

		dbs, err := bariloche.GenerateDatabases() //adapt to the new type
		if err != nil {
			log.Fatalln(err)
		}

		for _, db := range dbs.Data {
			schemas, err := bariloche.GenerateSchema(db.ID())
			if err != nil {
				log.Fatalln(err)
			}
			for _, schema := range schemas.Data {
				bariloche.GenerateTables(db.ID(), schema.ID()) //TODO: continue from here
				bariloche.GeneratePipes(db.ID(), schema.ID())
				bariloche.GenerateStages(db.ID(), schema.ID())
				bariloche.GenerateViews(db.ID(), schema.ID())
			}
		}

		bariloche.GenerateUsers()
		bariloche.GenerateRoles()
		bariloche.GenerateWareshouses()

	},
}

func init() {
	rootCmd.AddCommand(generateAllCmd)
}
