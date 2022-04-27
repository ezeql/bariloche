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

// connTestCmd represents the connTest command
var connTestCmd = &cobra.Command{
	Use:   "connTest",
	Short: "checks connection to Snowflake",
	Long:  `checks connection to Snowflake`,
	Run: func(cmd *cobra.Command, args []string) {

		db, err := bariloche.GetDB()
		if err != nil {
			log.Fatal(err)
		}

		defer db.Close()
		query := "SELECT 1"
		rows, err := db.Query(query)
		if err != nil {
			log.Fatalf("failed to run a query. %v, err: %v", query, err)
		}
		defer rows.Close()
		var v int
		for rows.Next() {
			err := rows.Scan(&v)
			if err != nil {
				log.Fatalf("failed to get result. err: %v", err)
			}
			if v != 1 {
				log.Fatalf("failed to get 1. got: %v", v)
			}
		}
		if rows.Err() != nil {
			fmt.Printf("ERROR: %v\n", rows.Err())
			return
		}
		fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
	},
}

func init() {
	rootCmd.AddCommand(connTestCmd)
}
