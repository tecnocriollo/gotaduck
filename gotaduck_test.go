package gotaduck

import (
	"database/sql"
	"testing"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	_ "github.com/marcboeker/go-duckdb"
)

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE test_table (
			id INTEGER,
			name VARCHAR,
			age INTEGER,
			score DOUBLE,
			is_active BOOLEAN
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO test_table (id, name, age, score, is_active)
		VALUES 
			(1, 'John', 25, 95.5, true),
			(2, 'Jane', 30, 88.0, false),
			(3, 'Bob', 35, 92.5, true)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	return db
}

func TestQueryToDataFrame(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test querying all data
	df, err := QueryToDataFrame(db, "SELECT * FROM test_table")
	if err != nil {
		t.Fatalf("QueryToDataFrame failed: %v", err)
	}

	// Check number of rows and columns
	if df.Nrow() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.Nrow())
	}
	if df.Ncol() != 5 {
		t.Errorf("Expected 5 columns, got %d", df.Ncol())
	}

	// Check column names
	expectedCols := []string{"id", "name", "age", "score", "is_active"}
	for i, col := range df.Names() {
		if col != expectedCols[i] {
			t.Errorf("Expected column %s, got %s", expectedCols[i], col)
		}
	}

	// Check data types
	if df.Col("id").Type() != series.Int {
		t.Error("Expected 'id' column to be Int type")
	}
	if df.Col("name").Type() != series.String {
		t.Error("Expected 'name' column to be String type")
	}
	if df.Col("age").Type() != series.Int {
		t.Error("Expected 'age' column to be Int type")
	}
	if df.Col("score").Type() != series.Float {
		t.Error("Expected 'score' column to be Float type")
	}
	if df.Col("is_active").Type() != series.Bool {
		t.Error("Expected 'is_active' column to be Bool type")
	}
}

func TestDataFrameToTable(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create a test DataFrame
	df := dataframe.New(
		series.New([]int{4, 5, 6}, series.Int, "id"),
		series.New([]string{"Alice", "Charlie", "David"}, series.String, "name"),
		series.New([]int{28, 32, 40}, series.Int, "age"),
		series.New([]float64{91.0, 87.5, 94.0}, series.Float, "score"),
		series.New([]bool{true, false, true}, series.Bool, "is_active"),
	)

	// Test creating a new table
	err := DataFrameToTable(db, df, "test_table_new")
	if err != nil {
		t.Fatalf("DataFrameToTable failed: %v", err)
	}

	// Verify the data was inserted correctly
	rows, err := db.Query("SELECT * FROM test_table_new")
	if err != nil {
		t.Fatalf("Failed to query new table: %v", err)
	}
	defer rows.Close()

	// Count rows
	count := 0
	for rows.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("Expected 3 rows in new table, got %d", count)
	}
}

func TestEmptyDataFrame(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Create an empty DataFrame
	df := dataframe.New(
		series.New([]int{}, series.Int, "id"),
		series.New([]string{}, series.String, "name"),
	)

	// Test creating a table with empty DataFrame
	err := DataFrameToTable(db, df, "empty_table")
	if err != nil {
		t.Fatalf("DataFrameToTable failed with empty DataFrame: %v", err)
	}

	// Verify the table was created but is empty
	rows, err := db.Query("SELECT COUNT(*) FROM empty_table")
	if err != nil {
		t.Fatalf("Failed to query empty table: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan count: %v", err)
		}
	}
	if count != 0 {
		t.Errorf("Expected 0 rows in empty table, got %d", count)
	}
} 