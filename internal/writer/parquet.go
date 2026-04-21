// Package writer provides a typed Parquet file writer for CDC output.
// Uses parquet-go (pure Go, no CGO).
package writer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	goparquet "github.com/parquet-go/parquet-go"
)

// Row is a generic row type for Parquet writing.
type Row = map[string]any

// WriteBaseParquet writes rows to a base Parquet file using parquet-go.
// All values are written as strings/bytes for simplicity in v0.1.
// Downstream consumers (pg-warehouse) handle type coercion via DuckDB's read_parquet.
func WriteBaseParquet(path string, rows []Row, columns []manifest.Column) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	// Filter out CDC metadata columns
	var dataCols []manifest.Column
	for _, col := range columns {
		if col.Name == btypes.ColOp || col.Name == btypes.ColEpoch || col.Name == btypes.ColDeletedAt {
			continue
		}
		dataCols = append(dataCols, col)
	}

	return writeParquetFile(path, rows, dataCols)
}

// WriteDeltaParquet writes CDC delta rows with __op and __epoch columns.
func WriteDeltaParquet(path string, rows []Row, columns []manifest.Column, epochNum int64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	// Prepend __op and __epoch columns
	allCols := []manifest.Column{
		{Name: btypes.ColOp, PgType: "text", ParquetType: "UTF8", Nullable: false},
		{Name: btypes.ColEpoch, PgType: "bigint", ParquetType: "INT64", Nullable: false},
	}
	allCols = append(allCols, columns...)

	// Inject __epoch into each row
	for i := range rows {
		rows[i][btypes.ColEpoch] = epochNum
	}

	return writeParquetFile(path, rows, allCols)
}

func writeParquetFile(path string, rows []Row, columns []manifest.Column) error {
	schema := goparquet.NewSchema("table", makeGroupNode(columns))

	// Build a name→schema column index map. Group is a map so the schema
	// may reorder columns vs. our slice; we must use the schema's indices.
	colIndex := make(map[string]int, len(columns))
	for _, col := range columns {
		leaf, ok := schema.Lookup(col.Name)
		if !ok {
			return fmt.Errorf("column %q not found in parquet schema", col.Name)
		}
		colIndex[col.Name] = leaf.ColumnIndex
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := goparquet.NewWriter(f, schema)

	for _, row := range rows {
		pqRow := make(goparquet.Row, 0, len(columns))
		for _, col := range columns {
			idx := colIndex[col.Name]
			val := toParquetValue(row[col.Name], col)
			defLevel := 0
			if col.Nullable && row[col.Name] != nil {
				defLevel = 1
			}
			pqRow = append(pqRow, val.Level(0, defLevel, idx))
		}
		// Row.Range assumes values are sorted by column index.
		sort.Slice(pqRow, func(i, j int) bool {
			return pqRow[i].Column() < pqRow[j].Column()
		})
		if _, err := w.WriteRows([]goparquet.Row{pqRow}); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}

	return w.Close()
}

func makeGroupNode(columns []manifest.Column) goparquet.Node {
	group := goparquet.Group{}
	for _, col := range columns {
		var node goparquet.Node
		switch btypes.ParquetType(col.PgType) {
		case "INT32":
			node = goparquet.Leaf(goparquet.Int32Type)
		case "INT64":
			node = goparquet.Leaf(goparquet.Int64Type)
		case "FLOAT":
			node = goparquet.Leaf(goparquet.FloatType)
		case "DOUBLE", "DECIMAL":
			node = goparquet.Leaf(goparquet.DoubleType)
		case "BOOLEAN":
			node = goparquet.Leaf(goparquet.BooleanType)
		case "BYTE_ARRAY":
			// bytea: raw binary, no logical type annotation.
			node = goparquet.Leaf(goparquet.ByteArrayType)
		default:
			// UTF8, JSON, UUID, TIMESTAMP_MICROS, DATE, TIME_MICROS and
			// anything else all round-trip as strings in v0.1 (see
			// WriteBaseParquet doc comment). Annotating them as STRING
			// makes DuckDB read them as VARCHAR instead of BLOB.
			node = goparquet.String()
		}
		if col.Nullable {
			node = goparquet.Optional(node)
		}
		group[col.Name] = node
	}
	return group
}

func toParquetValue(val any, col manifest.Column) goparquet.Value {
	if val == nil {
		return goparquet.NullValue()
	}

	pqType := btypes.ParquetType(col.PgType)
	switch pqType {
	case "INT32":
		return goparquet.ValueOf(toInt32(val))
	case "INT64":
		return goparquet.ValueOf(toInt64(val))
	case "FLOAT":
		return goparquet.ValueOf(toFloat32(val))
	case "DOUBLE", "DECIMAL":
		return goparquet.ValueOf(toFloat64(val))
	case "BOOLEAN":
		return goparquet.ValueOf(toBool(val))
	case "BYTE_ARRAY":
		return goparquet.ByteArrayValue(toBytes(val))
	default:
		return goparquet.ByteArrayValue([]byte(toString(val)))
	}
}

func toInt32(v any) int32 {
	switch val := v.(type) {
	case int32:
		return val
	case int64:
		return int32(val)
	case int:
		return int32(val)
	case float64:
		return int32(val)
	case string:
		// CDC decoder emits all values as strings (postgres text format).
		n, _ := strconv.ParseInt(val, 10, 32)
		return int32(n)
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int32:
		return int64(val)
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case string:
		n, _ := strconv.ParseInt(val, 10, 64)
		return n
	default:
		return 0
	}
}

func toFloat32(v any) float32 {
	switch val := v.(type) {
	case float32:
		return val
	case float64:
		return float32(val)
	case string:
		f, _ := strconv.ParseFloat(val, 32)
		return float32(f)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case json.Number:
		f, _ := val.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}

func toBool(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		// Postgres text format for bool is "t" / "f"; ParseBool handles both.
		b, _ := strconv.ParseBool(val)
		return b
	default:
		return false
	}
}

func toBytes(v any) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}

func toString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", val)
	}
}

