package columndata

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/go-gota/gota/series"
)

// ColumnData holds metadata and values for a database column
type ColumnData struct {
	Name     string
	ScanType reflect.Type
	Pointer  interface{}
	Data     interface{}
}

// InitializeColumns prepares the column metadata and scan pointers
func InitializeColumns(columns []*sql.ColumnType) ([]ColumnData, error) {
	cols := make([]ColumnData, len(columns))

	for i, col := range columns {
		scanType := col.ScanType()
		if scanType == nil {
			return nil, fmt.Errorf("unsupported column type for column: %s", col.Name())
		}

		cols[i] = ColumnData{
			Name:     col.Name(),
			ScanType: scanType,
			Pointer:  reflect.New(scanType).Interface(),
			Data:     reflect.MakeSlice(reflect.SliceOf(scanType), 0, 0).Interface(),
		}
	}
	return cols, nil
}

// AppendValue adds a new value to the column's data slice
func AppendValue(col *ColumnData) {
	colData := reflect.ValueOf(col.Data)
	colValue := reflect.ValueOf(col.Pointer).Elem()
	col.Data = reflect.Append(colData, colValue).Interface()
}

// CreateSeries converts column data into a Gota series
func CreateSeries(col ColumnData) (series.Series, error) {
	colValue := reflect.ValueOf(col.Data)
	elemKind := colValue.Type().Elem().Kind()

	switch elemKind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return convertIntegerSeries(col)
	case reflect.Float32, reflect.Float64:
		return convertFloatSeries(col)
	case reflect.String:
		return series.New(col.Data.([]string), series.String, col.Name), nil
	case reflect.Bool:
		return series.New(col.Data.([]bool), series.Bool, col.Name), nil
	case reflect.Struct:
		if colValue.Type().Elem().String() == "time.Time" {
			return convertTimeSeries(col)
		}
		return series.Series{}, fmt.Errorf("unsupported struct type for column: %s", col.Name)
	default:
		return series.Series{}, fmt.Errorf("unsupported data type for column: %s", col.Name)
	}
}

// private helper functions
func convertIntegerSeries(col ColumnData) (series.Series, error) {
	colValue := reflect.ValueOf(col.Data)
	elemType := colValue.Type().Elem()

	switch elemType.Kind() {
	case reflect.Int32:
		int32Slice := col.Data.([]int32)
		intValues := make([]int, len(int32Slice))
		for i, v := range int32Slice {
			intValues[i] = int(v)
		}
		return series.New(intValues, series.Int, col.Name), nil
	case reflect.Int64:
		int64Slice := col.Data.([]int64)
		intValues := make([]int, len(int64Slice))
		for i, v := range int64Slice {
			intValues[i] = int(v)
		}
		return series.New(intValues, series.Int, col.Name), nil
	default:
		// Handle other integer types if needed
		return series.Series{}, fmt.Errorf("unsupported integer type for column %s: %v", col.Name, elemType)
	}
}

func convertFloatSeries(col ColumnData) (series.Series, error) {
	colValue := reflect.ValueOf(col.Data)
	elemType := colValue.Type().Elem()

	switch elemType.Kind() {
	case reflect.Float32:
		float32Slice := col.Data.([]float32)
		floatValues := make([]float64, len(float32Slice))
		for i, v := range float32Slice {
			floatValues[i] = float64(v)
		}
		return series.New(floatValues, series.Float, col.Name), nil
	case reflect.Float64:
		return series.New(col.Data.([]float64), series.Float, col.Name), nil
	default:
		return series.Series{}, fmt.Errorf("unsupported float type for column %s: %v", col.Name, elemType)
	}
}

func convertTimeSeries(col ColumnData) (series.Series, error) {
	timeSlice := col.Data.([]time.Time)
	strValues := make([]string, len(timeSlice))
	for i, t := range timeSlice {
		strValues[i] = t.Format(time.RFC3339)
	}
	return series.New(strValues, series.String, col.Name), nil
}
