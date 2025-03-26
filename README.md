# Gotaduck

Gotaduck is a Go library designed to facilitate the seamless integration of DuckDB data with Gota dataframes. It provides an easy-to-use interface for reading data from DuckDB and converting it into Gota dataframes for further analysis and manipulation.

## Features

- **DuckDB Integration**: Read data directly from DuckDB databases.
- **Gota Dataframe Support**: Convert DuckDB query results into Gota dataframes.
- **Efficient and Lightweight**: Built with performance and simplicity in mind.

## Use Case

Gotaduck is ideal for developers and data scientists who work with DuckDB for data storage and Gota for data manipulation in Go. It bridges the gap between these two powerful tools, enabling efficient data workflows.

## Installation

To install Gotaduck, use:

```bash
go get github.com/tecnocriollo/gotaduck
```

## Example Usage

```go
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

	log.Println(gotaduck.QueryToDataFrame(db, `SELECT * FROM products`))
	log.Println(gotaduck.QueryToDataFrame(db, `SELECT max(price) FROM products`))

}

```

## License

This project is licensed under the MIT License. See the LICENSE file for details.
