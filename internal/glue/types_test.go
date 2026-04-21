package glue

import (
	"testing"

	"github.com/burnside-project/burnside-go/manifest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHiveType(t *testing.T) {
	cases := []struct {
		parquet string
		prec    int
		scale   int
		want    string
	}{
		{"INT32", 0, 0, "int"},
		{"INT64", 0, 0, "bigint"},
		{"BOOLEAN", 0, 0, "boolean"},
		{"FLOAT", 0, 0, "float"},
		{"DOUBLE", 0, 0, "double"},
		{"DECIMAL", 10, 2, "decimal(10,2)"},
		{"DECIMAL", 0, 0, "decimal(38,0)"},
		{"UTF8", 0, 0, "string"},
		{"JSON", 0, 0, "string"},
		{"UUID", 0, 0, "string"},
		{"TIMESTAMP_MICROS", 0, 0, "timestamp"},
		{"DATE", 0, 0, "date"},
		{"TIME_MICROS", 0, 0, "string"},
		{"BYTE_ARRAY", 0, 0, "binary"},
		{"UNKNOWN_TYPE", 0, 0, "string"},
	}
	for _, c := range cases {
		t.Run(c.parquet, func(t *testing.T) {
			assert.Equal(t, c.want, hiveType(c.parquet, c.prec, c.scale))
		})
	}
}

func TestGlueTableName(t *testing.T) {
	assert.Equal(t, "users", glueTableName("public", "users"))
	assert.Equal(t, "users", glueTableName("", "users"))
	assert.Equal(t, "billing_invoices", glueTableName("billing", "invoices"))
}

func TestTableInput(t *testing.T) {
	mt := manifest.Table{Status: "active", Path: "public.users.db/"}
	ms := manifest.Schema{
		Version: 3,
		Columns: []manifest.Column{
			{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
			{Name: "email", PgType: "text", ParquetType: "UTF8", Nullable: false},
			{Name: "balance", PgType: "numeric(10,2)", ParquetType: "DECIMAL", Precision: 10, Scale: 2, Nullable: true},
		},
	}

	in, err := tableInput("public.users", mt, ms, "s3://bucket/prefix/public.users.db/")
	require.NoError(t, err)

	assert.Equal(t, "users", *in.Name)
	assert.Equal(t, "EXTERNAL_TABLE", *in.TableType)
	assert.Equal(t, "parquet", in.Parameters["classification"])
	assert.Equal(t, "public", in.Parameters["pg_cdc.pg_schema"])
	assert.Equal(t, "users", in.Parameters["pg_cdc.pg_table"])
	assert.Equal(t, "3", in.Parameters["pg_cdc.schema_version"])

	require.Len(t, in.StorageDescriptor.Columns, 3)
	assert.Equal(t, "id", *in.StorageDescriptor.Columns[0].Name)
	assert.Equal(t, "bigint", *in.StorageDescriptor.Columns[0].Type)
	assert.Equal(t, "decimal(10,2)", *in.StorageDescriptor.Columns[2].Type)
	assert.Equal(t, "s3://bucket/prefix/public.users.db/", *in.StorageDescriptor.Location)
}

func TestTableInput_BadKey(t *testing.T) {
	_, err := tableInput("notqualified", manifest.Table{}, manifest.Schema{}, "s3://x/")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema.table")
}