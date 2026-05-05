package main

import (
	"github.com/burnside-project/pg-cdc-core/internal/mcpserver"
	"github.com/burnside-project/pg-cdc-core/internal/registry"
	"github.com/spf13/cobra"
)

func mcpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mcp",
		Short: "Serve a local MCP endpoint over the Parquet output (stdio, single-user, read-only)",
		Long: `Serve a Model Context Protocol endpoint over stdio so Claude Desktop, Cursor,
and other MCP clients can query the Parquet output written by 'pg-cdc init'
and 'pg-cdc start'.

The open-core MCP server is intentionally scoped:
  - stdio transport only (the client launches it as a subprocess)
  - single-user, read-only, no authentication, no network listener
  - tools: list_tables, describe_table, query, recent_changes

For multi-user / authenticated / governed MCP, see the commercial edition.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			sink, err := registry.NewSink(cfg.Storage)
			if err != nil {
				return err
			}
			return mcpserver.ServeStdio(cmd.Context(), cfg, sink)
		},
	}
}
