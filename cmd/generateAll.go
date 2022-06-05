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
		bariloche.GenerateUsers()

		fmt.Println("GenerateRoles")
		bariloche.GenerateRoles()

		fmt.Println("GenerateWareshouses")
		bariloche.GenerateWareshouses()

		fmt.Println("GenerateDatabases")
		dbs, err := bariloche.GenerateDatabases() //adapt to the new type
		if err != nil {
			log.Fatalln(err)
		}

		for _, db := range dbs.Data {
			fmt.Println("GenerateSchema")
			schemas, err := bariloche.GenerateSchema(db.ID())
			if err != nil {
				log.Fatalf("couldn't generates schema : %v\n", err)
			}
			for _, schema := range schemas.Data {
				fmt.Println("GenerateTables")
				_, err := bariloche.GenerateTables(db.ID(), schema.ResourceName())
				if err != nil {
					log.Fatalf("couldn't generates table: %v\n", err)
				}

				fmt.Println("GeneratePipes")
				_, err = bariloche.GeneratePipes(db.ID(), schema.ResourceName())
				if err != nil {
					log.Fatalln(err)
				}

				_, err = bariloche.GenerateStages(db.ID(), schema.ResourceName())
				if err != nil {
					log.Fatalln(err)
				}

				_, err = bariloche.GenerateViews(db.ID(), schema.ResourceName())
				if err != nil {
					log.Fatalln(err)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(generateAllCmd)
}
