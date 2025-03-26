package gotaduck

import (
	"database/sql"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/tecnocriollo/gotaduck/internal/columndata"
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

	cols, err := columndata.InitializeColumns(columns)
	if err != nil {
		return dataframe.DataFrame{}, err
	}

	// Create slice of scan pointers
	scanPointers := make([]interface{}, len(cols))
	for i := range cols {
		scanPointers[i] = cols[i].Pointer
	}

	// Process rows
	for rows.Next() {
		if err := rows.Scan(scanPointers...); err != nil {
			return dataframe.DataFrame{}, err
		}

		for i := range cols {
			columndata.AppendValue(&cols[i])
		}
	}

	// Create series list
	seriesList := make([]series.Series, 0, len(cols))
	for _, col := range cols {
		s, err := columndata.CreateSeries(col)
		if err != nil {
			return dataframe.DataFrame{}, err
		}
		seriesList = append(seriesList, s)
	}

	return dataframe.New(seriesList...), nil
}

func DataFrameToTable(db *sql.DB, df dataframe.DataFrame, tableName string) error {
	// Create table based on DataFrame structure
	createQuery := generateCreateTableSQL(df, tableName)
	_, err := db.Exec(createQuery)
	if err != nil {
		return err
	}

	// Insert data
	records := df.Records()
	if len(records) <= 1 { // Empty dataframe or only headers
		return nil
	}

	// Prepare insert statement
	placeholders := make([]string, len(records[0]))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := "INSERT INTO " + tableName + " VALUES (" + strings.Join(placeholders, ", ") + ")"
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Insert each row
	for i := 1; i < len(records); i++ {
		values := make([]interface{}, len(records[i]))
		for j := range records[i] {
			values[j] = records[i][j]
		}
		_, err = stmt.Exec(values...)
		if err != nil {
			return err
		}
	}

	return nil
}

func generateCreateTableSQL(df dataframe.DataFrame, tableName string) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE " + tableName + " (")

	cols := df.Names()
	types := make([]string, len(cols))
	for i, col := range cols {
		types[i] = inferSQLType(df.Col(col))
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col + " " + types[i])
	}
	b.WriteString(")")
	return b.String()
}

func inferSQLType(s series.Series) string {
	switch s.Type() {
	case series.Int:
		return "INTEGER"
	case series.Float:
		return "REAL"
	case series.String:
		return "TEXT"
	case series.Bool:
		return "BOOLEAN"
	default:
		return "TEXT"
	}
}
