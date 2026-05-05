package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

func toolDefinitions() []tool {
	return []tool{
		{
			Name:        "list_tables",
			Description: "List active tables from the pg-cdc manifest with row counts, tags, and epochs.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			Name:        "describe_table",
			Description: "Show the schema (columns, Postgres types, nullability) for a single table.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{
						"type":        "string",
						"description": "Fully-qualified table name (e.g. \"public.orders\").",
					},
				},
				"required": []string{"name"},
			},
		},
		{
			Name:        "query",
			Description: "Run a read-only SELECT against the Parquet snapshot for a table. Returns rows as JSON.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"sql":   map[string]any{"type": "string", "description": "SELECT statement against pg-cdc Parquet tables."},
					"limit": map[string]any{"type": "integer", "description": "Max rows to return (default 100, max 1000)."},
				},
				"required": []string{"sql"},
			},
		},
		{
			Name:        "recent_changes",
			Description: "Return the N most recent rows from the streaming delta files for a table.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":  map[string]any{"type": "string", "description": "Fully-qualified table name."},
					"limit": map[string]any{"type": "integer", "description": "Max rows (default 20, max 200)."},
				},
				"required": []string{"name"},
			},
		},
	}
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func (s *server) callTool(ctx context.Context, raw json.RawMessage) map[string]any {
	var p toolCallParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return errorContent(fmt.Errorf("invalid tools/call params: %w", err))
	}
	switch p.Name {
	case "list_tables":
		return s.listTables(ctx)
	case "describe_table":
		return s.describeTable(ctx, p.Arguments)
	case "query":
		return s.query(ctx, p.Arguments)
	case "recent_changes":
		return s.recentChanges(ctx, p.Arguments)
	default:
		return errorContent(fmt.Errorf("unknown tool: %s", p.Name))
	}
}

func (s *server) listTables(ctx context.Context) map[string]any {
	m, err := s.sink.ReadManifest(ctx)
	if err != nil {
		return errorContent(fmt.Errorf("read manifest: %w (run `pg-cdc init` first?)", err))
	}
	active := m.ActiveTables()
	names := make([]string, 0, len(active))
	for n := range active {
		names = append(names, n)
	}
	sort.Strings(names)

	var b strings.Builder
	fmt.Fprintf(&b, "%d active tables (sink: %s)\n\n", len(names), s.sink.Backend())
	for _, n := range names {
		t := active[n]
		fmt.Fprintf(&b, "- %s · rows=%d · base_epoch=%d · latest_delta=%d",
			n, t.RowCount, t.BaseEpoch, t.LatestDeltaEpoch)
		if len(t.Tags) > 0 {
			fmt.Fprintf(&b, " · tags=[%s]", strings.Join(t.Tags, ","))
		}
		b.WriteString("\n")
	}
	return textContent(b.String())
}

func (s *server) describeTable(ctx context.Context, args json.RawMessage) map[string]any {
	var a struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return errorContent(fmt.Errorf("invalid arguments: %w", err))
	}
	if a.Name == "" {
		return errorContent(fmt.Errorf("name is required"))
	}
	m, err := s.sink.ReadManifest(ctx)
	if err != nil {
		return errorContent(fmt.Errorf("read manifest: %w", err))
	}
	t, ok := m.Tables[a.Name]
	if !ok {
		return errorContent(fmt.Errorf("table not found: %s", a.Name))
	}
	sch, ok := m.Schemas[a.Name]
	if !ok {
		return errorContent(fmt.Errorf("no schema recorded for %s", a.Name))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s · status=%s · rows=%d · schema_version=%d\n\n",
		a.Name, t.Status, t.RowCount, sch.Version)
	if len(t.PrimaryKey) > 0 {
		fmt.Fprintf(&b, "primary key: %s\n\n", strings.Join(t.PrimaryKey, ", "))
	}
	fmt.Fprintln(&b, "columns:")
	for _, c := range sch.Columns {
		nullable := "NOT NULL"
		if c.Nullable {
			nullable = "NULL"
		}
		fmt.Fprintf(&b, "  %-30s %-20s %s\n", c.Name, c.PgType, nullable)
	}
	return textContent(b.String())
}

func textContent(s string) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": s}},
		"isError": false,
	}
}

func errorContent(err error) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": err.Error()}},
		"isError": true,
	}
}
