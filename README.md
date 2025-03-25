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

	_ "github.com/marcboeker/go-duckdb"
	"github.com/tecnocriollo/gotaduck"
)

func main() {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	df, err := gotaduck.QueryToDataFrame(db, `SELECT id, name FROM 'people.csv'`)
	if err != nil {
		log.Fatal(err)
	}
	// Print the DataFrame
	log.Println(df)

}

```

## License

This project is licensed under the MIT License. See the LICENSE file for details.
