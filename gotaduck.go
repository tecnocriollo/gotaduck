package gotaduck

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/tecnocriollo/gotaduck/internal/columndata"
)

// Error definitions
var ErrNotDuckDBConnection = errors.New("not a DuckDB connection")

// isDuckDBConnection checks if the provided database connection is a DuckDB connection
func isDuckDBConnection(db *sql.DB) bool {
	if db == nil {
		return false
	}

	// Check if the driver type contains "duckdb"
	driverType := fmt.Sprintf("%T", db.Driver())
	return strings.Contains(strings.ToLower(driverType), "duckdb")
}

func QueryToDataFrame(db *sql.DB, query string) (dataframe.DataFrame, error) {
	if !isDuckDBConnection(db) {
		return dataframe.DataFrame{}, ErrNotDuckDBConnection
	}

	// Create a prepared statement to prevent SQL injection
	stmt, err := db.Prepare(query)
	if err != nil {
		return dataframe.DataFrame{}, err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
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

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return dataframe.DataFrame{}, err
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

func QueryToDataFrameContext(ctx context.Context, db *sql.DB, query string) (dataframe.DataFrame, error) {
	if !isDuckDBConnection(db) {
		return dataframe.DataFrame{}, ErrNotDuckDBConnection
	}

	rows, err := db.QueryContext(ctx, query)
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

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return dataframe.DataFrame{}, err
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

func validateTableName(name string) error {
	// Only allow alphanumeric and underscore
	for _, char := range name {
		if !unicode.IsLetter(char) && !unicode.IsDigit(char) && char != '_' {
			return fmt.Errorf("invalid table name: only alphanumeric and underscore allowed")
		}
	}
	return nil
}

func validateColumnName(name string) error {
	if name == "" {
		return fmt.Errorf("empty column name not allowed")
	}
	// Only allow alphanumeric and underscore
	for _, char := range name {
		if !unicode.IsLetter(char) && !unicode.IsDigit(char) && char != '_' {
			return fmt.Errorf("invalid column name: %s", name)
		}
	}
	return nil
}

func DataFrameToTable(db *sql.DB, df dataframe.DataFrame, tableName string) error {
	if !isDuckDBConnection(db) {
		return ErrNotDuckDBConnection
	}

	if err := validateTableName(tableName); err != nil {
		return err
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	// Defer a rollback in case anything fails
	defer func() {
		if err := recover(); err != nil {
			tx.Rollback()
			panic(err) // re-throw panic after rollback
		}
	}()

	// Create table based on DataFrame structure
	createQuery, err := generateCreateTableSQL(df, tableName)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to generate create table SQL: %w", err)
	}

	_, err = tx.Exec(createQuery)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data
	records := df.Records()
	if len(records) <= 1 { // Empty dataframe or only headers
		return tx.Commit()
	}

	// Prepare insert statement
	placeholders := make([]string, len(records[0]))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO \"%s\" VALUES (%s)",
		strings.ReplaceAll(tableName, "\"", "\"\""),
		strings.Join(placeholders, ", "))

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert each row within the transaction
	for i := 1; i < len(records); i++ {
		values := make([]interface{}, len(records[i]))
		for j := range records[i] {
			values[j] = records[i][j]
		}
		_, err = stmt.Exec(values...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert row %d: %w", i, err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func generateCreateTableSQL(df dataframe.DataFrame, tableName string) (string, error) {
	var b strings.Builder
	b.WriteString("CREATE TABLE " + tableName + " (")

	cols := df.Names()
	for i, col := range cols {
		if err := validateColumnName(col); err != nil {
			return "", err
		}
		types := inferSQLType(df.Col(col))
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col + " " + types)
	}
	b.WriteString(")")
	return b.String(), nil
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
