package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"
	"github.com/burnside-project/pg-cdc-core/internal/config"
)

// fakeSink is a minimal ports.Sink for tests. Only ReadManifest and Backend
// are exercised by the MCP server; the rest are no-ops.
type fakeSink struct {
	m *manifest.Manifest
}

func (f *fakeSink) ReadManifest(context.Context) (*manifest.Manifest, error)  { return f.m, nil }
func (f *fakeSink) WriteManifest(context.Context, *manifest.Manifest) error   { return nil }
func (f *fakeSink) WriteFile(context.Context, string, io.Reader) error        { return nil }
func (f *fakeSink) ReadFile(context.Context, string) (io.ReadCloser, error)   { return nil, nil }
func (f *fakeSink) ListFiles(context.Context, string) ([]string, error)       { return nil, nil }
func (f *fakeSink) DeleteFile(context.Context, string) error                  { return nil }
func (f *fakeSink) Backend() string                                           { return "filesystem" }

func newTestServer(storagePath string) *server {
	return &server{
		cfg: config.Config{Storage: config.StorageConfig{Type: "filesystem", Path: storagePath}},
		sink: &fakeSink{m: &manifest.Manifest{
			Version: 1,
			Tables: map[string]manifest.Table{
				"public.orders": {
					Status:           "active",
					PrimaryKey:       []string{"id"},
					Path:             "public.orders",
					RowCount:         10,
					BaseEpoch:        0,
					LatestDeltaEpoch: 1,
				},
			},
			Schemas: map[string]manifest.Schema{
				"public.orders": {Version: 1, Columns: []manifest.Column{
					{Name: "id", PgType: "bigint", ParquetType: "INT64"},
					{Name: "customer_id", PgType: "text", ParquetType: "BYTE_ARRAY", Nullable: true},
				}},
			},
		}},
	}
}

// rpc round-trips a single newline-delimited request through the server's
// serve loop and returns the parsed response.
func rpc(t *testing.T, s *server, req string) rpcResponse {
	t.Helper()
	var out bytes.Buffer
	if err := s.serve(context.Background(), strings.NewReader(req+"\n"), &out); err != nil {
		t.Fatalf("serve: %v", err)
	}
	if out.Len() == 0 {
		return rpcResponse{}
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response %q: %v", out.String(), err)
	}
	return resp
}

// resultText extracts the first text content from a tool result.
func resultText(t *testing.T, resp rpcResponse) (text string, isError bool) {
	t.Helper()
	raw, _ := json.Marshal(resp.Result)
	var got struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal tool result: %v", err)
	}
	if len(got.Content) == 0 {
		t.Fatal("tool result has no content")
	}
	return got.Content[0].Text, got.IsError
}

func TestInitialize(t *testing.T) {
	s := newTestServer(t.TempDir())
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result is not a map: %v", resp.Result)
	}
	if got := result["protocolVersion"]; got != protocolVersion {
		t.Errorf("protocolVersion = %v, want %v", got, protocolVersion)
	}
	info, _ := result["serverInfo"].(map[string]any)
	if info["name"] != "pg-cdc" {
		t.Errorf("serverInfo.name = %v, want pg-cdc", info["name"])
	}
}

func TestToolsList(t *testing.T) {
	s := newTestServer(t.TempDir())
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	raw, _ := json.Marshal(resp.Result)
	var got struct {
		Tools []tool `json:"tools"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal tools: %v", err)
	}
	want := []string{"list_tables", "describe_table", "query", "recent_changes"}
	if len(got.Tools) != len(want) {
		t.Fatalf("tools count = %d, want %d", len(got.Tools), len(want))
	}
	for i, n := range want {
		if got.Tools[i].Name != n {
			t.Errorf("tool[%d].name = %q, want %q", i, got.Tools[i].Name, n)
		}
		if got.Tools[i].InputSchema == nil {
			t.Errorf("tool[%d] (%s) has no inputSchema", i, n)
		}
	}
}

func TestListTables(t *testing.T) {
	s := newTestServer(t.TempDir())
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_tables"}}`)
	text, isErr := resultText(t, resp)
	if isErr {
		t.Errorf("unexpected error: %s", text)
	}
	if !strings.Contains(text, "public.orders") {
		t.Errorf("expected public.orders in output, got: %s", text)
	}
	if !strings.Contains(text, "rows=10") {
		t.Errorf("expected rows=10 in output, got: %s", text)
	}
}

func TestDescribeTable(t *testing.T) {
	s := newTestServer(t.TempDir())
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"describe_table","arguments":{"name":"public.orders"}}}`)
	text, isErr := resultText(t, resp)
	if isErr {
		t.Errorf("unexpected error: %s", text)
	}
	for _, want := range []string{"id", "bigint", "customer_id", "primary key"} {
		if !strings.Contains(text, want) {
			t.Errorf("expected %q in describe output, got: %s", want, text)
		}
	}
}

func TestDescribeTableNotFound(t *testing.T) {
	s := newTestServer(t.TempDir())
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"describe_table","arguments":{"name":"public.nope"}}}`)
	_, isErr := resultText(t, resp)
	if !isErr {
		t.Errorf("expected isError=true for missing table")
	}
}

func TestValidateSelect(t *testing.T) {
	cases := []struct {
		sql string
		ok  bool
	}{
		{"SELECT 1", true},
		{"select * from t", true},
		{"WITH x AS (SELECT 1) SELECT * FROM x", true},
		{"  SELECT * FROM t ;", true},
		{"INSERT INTO t VALUES (1)", false},
		{"DROP TABLE t", false},
		{"SELECT 1; DELETE FROM t", false},
		{"PRAGMA database_list", false},
		{"ATTACH 'foo.db'", false},
		{"", false},
	}
	for _, c := range cases {
		err := validateSelect(c.sql)
		if c.ok && err != nil {
			t.Errorf("%q: unexpected error %v", c.sql, err)
		}
		if !c.ok && err == nil {
			t.Errorf("%q: expected error, got nil", c.sql)
		}
	}
}

func TestUnknownMethod(t *testing.T) {
	s := newTestServer(t.TempDir())
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"foo/bar"}`)
	if resp.Error == nil {
		t.Fatalf("expected error for unknown method")
	}
	if resp.Error.Code != -32601 {
		t.Errorf("error code = %d, want -32601", resp.Error.Code)
	}
}

func TestNotificationNoResponse(t *testing.T) {
	s := newTestServer(t.TempDir())
	var out bytes.Buffer
	in := strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n")
	if err := s.serve(context.Background(), in, &out); err != nil {
		t.Fatalf("serve: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("expected no response for notification, got: %s", out.String())
	}
}

// TestQueryEndToEnd exercises the full query path against real Parquet files.
// Skips if the DuckDB CLI isn't installed — protocol-level tests cover the rest.
func TestQueryEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb CLI not on PATH — skipping end-to-end query test")
	}
	dir := t.TempDir()

	// Write a real parquet file via DuckDB.
	tableDir := filepath.Join(dir, "public.orders", "base")
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	pq := filepath.Join(tableDir, "base_000000.parquet")
	create := fmt.Sprintf(
		"COPY (SELECT i AS id, ('cust-' || i) AS customer_id FROM range(1,4) t(i)) "+
			"TO '%s' (FORMAT PARQUET)", pq)
	if err := exec.Command("duckdb", "-c", create).Run(); err != nil {
		t.Fatalf("create test parquet: %v", err)
	}

	s := newTestServer(dir)
	resp := rpc(t, s, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query","arguments":{"sql":"SELECT id, customer_id FROM public.orders ORDER BY id"}}}`)
	text, isErr := resultText(t, resp)
	if isErr {
		t.Fatalf("query returned error: %s", text)
	}
	if !strings.Contains(text, "cust-1") || !strings.Contains(text, "cust-3") {
		t.Errorf("expected cust-1..cust-3 in result, got: %s", text)
	}
}
