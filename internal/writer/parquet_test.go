package writer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"
	goparquet "github.com/parquet-go/parquet-go"
)

func TestWriteBaseParquet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "public.orders", "base", "base_000000.parquet")

	columns := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		{Name: "name", PgType: "text", ParquetType: "UTF8", Nullable: false},
		{Name: "price", PgType: "numeric(10,2)", ParquetType: "DECIMAL", Nullable: true},
	}

	rows := []Row{
		{"id": int64(1), "name": "Widget", "price": 9.99},
		{"id": int64(2), "name": "Gadget", "price": 29.99},
		{"id": int64(3), "name": "Gizmo", "price": nil},
	}

	if err := WriteBaseParquet(path, rows, columns); err != nil {
		t.Fatalf("WriteBaseParquet: %v", err)
	}

	// Verify file exists
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("file not found: %v", err)
	}
	if info.Size() == 0 {
		t.Error("file is empty")
	}

	// Read back with parquet-go
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	stat, _ := f.Stat()
	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	if pf.NumRows() != 3 {
		t.Errorf("rows = %d, want 3", pf.NumRows())
	}
}

func TestWriteBaseParquet_NullableByteArray(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "public.addresses", "base", "base_000000.parquet")

	columns := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		{Name: "city", PgType: "text", ParquetType: "UTF8", Nullable: true},
		{Name: "zip", PgType: "text", ParquetType: "UTF8", Nullable: true},
	}

	rows := []Row{
		{"id": int64(1), "city": "Portland", "zip": "97201"},
		{"id": int64(2), "city": nil, "zip": "10001"},
		{"id": int64(3), "city": "Seattle", "zip": nil},
	}

	if err := WriteBaseParquet(path, rows, columns); err != nil {
		t.Fatalf("WriteBaseParquet with nullable text: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	stat, _ := f.Stat()
	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	if pf.NumRows() != 3 {
		t.Errorf("rows = %d, want 3", pf.NumRows())
	}
}

// TestWriteDeltaParquet_StringValuesFromCDC reproduces the CDC stream path:
// the pgoutput decoder emits every column as a Go string, so the writer's
// type coercers must parse strings into the parquet column type. Previously
// they did not, so every numeric/bool field (including PKs) was written as 0,
// causing 1000 inserts to collapse onto a single PK during compaction.
func TestWriteDeltaParquet_StringValuesFromCDC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deltas", "epoch=000001.parquet")

	columns := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		{Name: "qty", PgType: "integer", ParquetType: "INT32", Nullable: false},
		{Name: "price", PgType: "numeric(10,2)", ParquetType: "DECIMAL", Nullable: false},
		{Name: "active", PgType: "boolean", ParquetType: "BOOLEAN", Nullable: false},
		{Name: "name", PgType: "text", ParquetType: "UTF8", Nullable: false},
	}

	// All values as strings, mirroring what postgres.decodeTuple produces.
	rows := []Row{
		{"__op": "I", "id": "101", "qty": "5", "price": "9.99", "active": "t", "name": "a"},
		{"__op": "I", "id": "102", "qty": "7", "price": "29.99", "active": "f", "name": "b"},
		{"__op": "I", "id": "103", "qty": "9", "price": "49.50", "active": "t", "name": "c"},
	}

	if err := WriteDeltaParquet(path, rows, columns, 1); err != nil {
		t.Fatalf("WriteDeltaParquet: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	stat, _ := f.Stat()
	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	type readCol struct {
		idx int
		typ string
	}
	cols := map[string]readCol{}
	for _, field := range pf.Schema().Fields() {
		leaf, _ := pf.Schema().Lookup(field.Name())
		cols[field.Name()] = readCol{idx: leaf.ColumnIndex, typ: field.Type().String()}
	}

	pqRows := make([]goparquet.Row, 3)
	for i := range pqRows {
		pqRows[i] = make(goparquet.Row, len(cols))
	}
	rgReader := pf.RowGroups()[0].Rows()
	n, _ := rgReader.ReadRows(pqRows)
	_ = rgReader.Close()
	if n != 3 {
		t.Fatalf("read %d rows, want 3", n)
	}

	wantIDs := []int64{101, 102, 103}
	wantQty := []int32{5, 7, 9}
	wantActive := []bool{true, false, true}
	idIdx := cols["id"].idx
	qtyIdx := cols["qty"].idx
	priceIdx := cols["price"].idx
	activeIdx := cols["active"].idx

	for i := 0; i < n; i++ {
		if got := pqRows[i][idIdx].Int64(); got != wantIDs[i] {
			t.Errorf("row %d id = %d, want %d (string PK was not parsed)", i, got, wantIDs[i])
		}
		if got := pqRows[i][qtyIdx].Int32(); got != wantQty[i] {
			t.Errorf("row %d qty = %d, want %d", i, got, wantQty[i])
		}
		if got := pqRows[i][priceIdx].Double(); got == 0 {
			t.Errorf("row %d price was zero — string decimal was not parsed", i)
		}
		if got := pqRows[i][activeIdx].Boolean(); got != wantActive[i] {
			t.Errorf("row %d active = %v, want %v (postgres 't'/'f' not parsed)", i, got, wantActive[i])
		}
	}
}

// TestWriteBaseParquet_StringLogicalType verifies that text/json/uuid/temporal
// columns get the UTF8 (STRING) logical-type annotation. Without it, DuckDB
// reads BYTE_ARRAY columns as BLOB instead of VARCHAR.
func TestWriteBaseParquet_StringLogicalType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "public.mixed", "base", "base_000000.parquet")

	columns := []manifest.Column{
		{Name: "id", PgType: "bigint", Nullable: false},
		{Name: "status", PgType: "text", Nullable: false},
		{Name: "meta", PgType: "jsonb", Nullable: true},
		{Name: "uid", PgType: "uuid", Nullable: true},
		{Name: "created_at", PgType: "timestamptz", Nullable: true},
		{Name: "blob_col", PgType: "bytea", Nullable: true},
	}
	rows := []Row{{"id": int64(1), "status": "ok", "meta": `{"a":1}`, "uid": "00000000-0000-0000-0000-000000000001", "created_at": "2026-04-17T00:00:00Z", "blob_col": []byte{0x00, 0x01}}}

	if err := WriteBaseParquet(path, rows, columns); err != nil {
		t.Fatalf("WriteBaseParquet: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	stat, _ := f.Stat()
	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	byName := map[string]goparquet.Field{}
	for _, field := range pf.Schema().Fields() {
		byName[field.Name()] = field
	}

	wantString := []string{"status", "meta", "uid", "created_at"}
	for _, name := range wantString {
		field, ok := byName[name]
		if !ok {
			t.Errorf("missing column %q", name)
			continue
		}
		lt := field.Type().LogicalType()
		if lt == nil || lt.UTF8 == nil {
			t.Errorf("column %q: want UTF8 logical type, got %v", name, lt)
		}
	}

	// bytea stays as raw BYTE_ARRAY (no logical type).
	if field, ok := byName["blob_col"]; ok {
		if lt := field.Type().LogicalType(); lt != nil {
			t.Errorf("bytea column: want no logical type, got %v", lt)
		}
	}
}

func TestWriteDeltaParquet(t *testing.T) {
	// ByteArray nil pointer is fixed — definition levels now set correctly.
	dir := t.TempDir()
	path := filepath.Join(dir, "deltas", "epoch=000001.parquet")

	columns := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
		{Name: "status", PgType: "text", ParquetType: "UTF8", Nullable: false},
	}

	rows := []Row{
		{"__op": "I", "id": int64(101), "status": "pending"},
		{"__op": "U", "id": int64(1), "status": "shipped"},
		{"__op": "D", "id": int64(50), "status": "deleted"},
	}

	if err := WriteDeltaParquet(path, rows, columns, 1); err != nil {
		t.Fatalf("WriteDeltaParquet: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	stat, _ := f.Stat()
	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	if pf.NumRows() != 3 {
		t.Errorf("rows = %d, want 3", pf.NumRows())
	}

	// Verify schema has __op and __epoch columns
	schema := pf.Schema()
	colNames := make(map[string]bool)
	for _, field := range schema.Fields() {
		colNames[field.Name()] = true
	}
	if !colNames["__op"] {
		t.Error("missing __op column")
	}
	if !colNames["__epoch"] {
		t.Error("missing __epoch column")
	}
	if !colNames["id"] {
		t.Error("missing id column")
	}
}
