package main

import (
	"database/sql"
	"log"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
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

	df_products := dataframe.New(
		series.New([]int{1001, 1002, 1003}, series.Int, "sku"),
		series.New([]float64{29.99, 15.50, 45.75}, series.Float, "price"),
		series.New([]int{100, 50, 75}, series.Int, "quantity"),
		series.New([]float64{0.30, 0.31, 0.61}, series.Float, "unit_price"),
	)
	gotaduck.DataFrameToTable(db, df_products, `products`)

	df2, err := gotaduck.QueryToDataFrame(db, `SELECT max(price) FROM products`)
	if err != nil {
		log.Fatal(err)
	}
	// Print the DataFrame
	log.Println(df2)
}
