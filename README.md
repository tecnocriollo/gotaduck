# Gotaduck

A Go package that provides integration between DuckDB and Gota DataFrames. 

WARNING:
This version it's not prepared to used in production environments. I hope in the future wil be.

## Features

- **DuckDB Integration**: Read data directly from DuckDB databases.
- **Gota Dataframe Support**: Convert DuckDB query results into Gota dataframes.
- **Efficient and Lightweight**: Built with performance and simplicity in mind.

## Use Case

Gotaduck is ideal for developers, data engineers and data scientists who work with DuckDB for data storage and Gota for data manipulation in Go. It bridges the gap between these two powerful tools, enabling efficient data workflows.

## Installation

```bash
go get github.com/tecnocriollo/gotaduck
```

## Supported Types

When converting DuckDB query results to Gota DataFrames, the following SQL types are supported:

| DuckDB Type | Go Type   | Gota Series Type |
|-------------|-----------|------------------|
| INTEGER     | int32     | series.Int      |
| BIGINT      | int64     | series.Int      |
| REAL        | float32   | series.Float    |
| DOUBLE      | float64   | series.Float    |
| VARCHAR     | string    | series.String   |
| BOOLEAN     | bool      | series.Bool     |
| DATE        | time.Time | series.String   |
| TIMESTAMP   | time.Time | series.String   |

Note: DATE and TIMESTAMP types are converted to strings using RFC3339 format.


## Examples

### Query to DataFrame
```go
df, err := gotaduck.QueryToDataFrame(db, `
    SELECT 
        CAST(1 AS INTEGER) as int_col,
        CAST(2.5 AS DOUBLE) as double_col,
        'text' as string_col,
        TRUE as bool_col,
        TIMESTAMP '2024-03-26 12:00:00' as timestamp_col
`)
```

### DataFrame to DuckDB Table
```go
// Create a sample DataFrame
df := dataframe.New(
    series.New([]int{1001, 1002, 1003}, series.Int, "product_id"),
    series.New([]string{"Laptop", "Mouse", "Keyboard"}, series.String, "name"),
    series.New([]float64{999.99, 25.50, 89.99}, series.Float, "price"),
    series.New([]bool{true, false, true}, series.Bool, "in_stock"),
)

// Save DataFrame to DuckDB table
err := gotaduck.DataFrameToTable(db, df, "products")
if err != nil {
    log.Fatal(err)
}

// Verify the data
result, err := gotaduck.QueryToDataFrame(db, "SELECT * FROM products")
if err != nil {
    log.Fatal(err)
}
fmt.Println(result)

// Output:
//    product_id     name  price in_stock
// 0       1001   Laptop 999.99     true
// 1       1002    Mouse  25.50    false
// 2       1003 Keyboard  89.99     true
```

The `DataFrameToTable` function:
- Automatically creates a table with appropriate column types
- Supports INTEGER, REAL, TEXT, and BOOLEAN columns
- Handles batch insertions efficiently
- Preserves column names from the DataFrame

For more examples, check the `examples/demo` directory.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
