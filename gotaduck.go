package gotaduck

import (
	"database/sql"
	"fmt"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func QueryToDataFrame(db *sql.DB, query string) (dataframe.DataFrame, error) {
	rows, err := db.Query(query)
	if err != nil {
		return dataframe.DataFrame{}, err
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return dataframe.DataFrame{}, err
	}

	// Prepare slices to hold the query results dynamically based on column types
	columnValues := make([]interface{}, len(columns))
	columnPointers := make([]interface{}, len(columns))
	for i, col := range columns {
		switch col.DatabaseTypeName() {
		case "BIGINT":
			var intValue int
			columnPointers[i] = &intValue
			columnValues[i] = intValue
		case "INTEGER":
			var intValue int
			columnPointers[i] = &intValue
			columnValues[i] = intValue
		case "VARCHAR":
			var stringValue string
			columnPointers[i] = &stringValue
			columnValues[i] = stringValue
		case "FLOAT", "DOUBLE":
			var floatValue float64
			columnPointers[i] = &floatValue
			columnValues[i] = floatValue
		case "BOOLEAN":
			var boolValue bool
			columnPointers[i] = &boolValue
			columnValues[i] = boolValue
		case "DATE":
			var dateValue string // DuckDB DATE can be handled as string
			columnPointers[i] = &dateValue
			columnValues[i] = dateValue
		case "TIMESTAMP":
			var timestampValue string // DuckDB TIMESTAMP can be handled as string
			columnPointers[i] = &timestampValue
			columnValues[i] = timestampValue
		default:
			return dataframe.DataFrame{}, fmt.Errorf("unsupported column type: %s", col.DatabaseTypeName())
		}
	}

	// Prepare slices to hold the final results
	data := make(map[string]interface{})
	for _, col := range columns {
		switch col.DatabaseTypeName() {
		case "BIGINT":
			data[col.Name()] = []int{}
		case "INTEGER":
			data[col.Name()] = []int{}
		case "VARCHAR":
			data[col.Name()] = []string{}
		case "FLOAT", "DOUBLE":
			data[col.Name()] = []float64{}
		case "BOOLEAN":
			data[col.Name()] = []bool{}
		case "DATE", "TIMESTAMP":
			data[col.Name()] = []string{}
		}
	}

	for rows.Next() {
		// Scan the row into the column pointers
		if err := rows.Scan(columnPointers...); err != nil {
			return dataframe.DataFrame{}, err
		}

		// Append values to the appropriate slices based on column order
		for i, col := range columns {
			switch col.DatabaseTypeName() {
			case "BIGINT":
				data[col.Name()] = append(data[col.Name()].([]int), *columnPointers[i].(*int))
			case "INTEGER":
				data[col.Name()] = append(data[col.Name()].([]int), *columnPointers[i].(*int))
			case "VARCHAR":
				data[col.Name()] = append(data[col.Name()].([]string), *columnPointers[i].(*string))
			case "FLOAT", "DOUBLE":
				data[col.Name()] = append(data[col.Name()].([]float64), *columnPointers[i].(*float64))
			case "BOOLEAN":
				data[col.Name()] = append(data[col.Name()].([]bool), *columnPointers[i].(*bool))
			case "DATE", "TIMESTAMP":
				data[col.Name()] = append(data[col.Name()].([]string), *columnPointers[i].(*string))
			}
		}
	}

	// Create a Gota DataFrame from the slices
	seriesList := []series.Series{}
	for colName, colData := range data {
		switch colData := colData.(type) {
		case []int:
			seriesList = append(seriesList, series.New(colData, series.Int, colName))
		case []string:
			seriesList = append(seriesList, series.New(colData, series.String, colName))
		case []float64:
			seriesList = append(seriesList, series.New(colData, series.Float, colName))
		case []bool:
			seriesList = append(seriesList, series.New(colData, series.Bool, colName))
		}
	}

	return dataframe.New(seriesList...), nil
}
