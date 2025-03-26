package gotaduck

import (
	"database/sql"

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
