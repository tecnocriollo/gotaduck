package main

import (
	"database/sql"
	"log"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/tecnocriollo/gotaduck"
)

func main() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	df, err := gotaduck.QueryToDataFrame(db, `SELECT TIMESTAMP '2025-03-03' as date, id, name FROM 'people.csv'`)
	if err != nil {
		log.Fatal(err)
	}
	// Print the DataFrame
	log.Println(df)

}
