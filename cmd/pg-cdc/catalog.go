package main

import (
	"github.com/burnside-project/pg-cdc-core/internal"
	"github.com/burnside-project/pg-cdc-core/internal/registry"
	"github.com/spf13/cobra"
)

func catalogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "catalog",
		Short: "Manage catalog (Glue) registration for an existing deployment",
	}
	cmd.AddCommand(catalogRegisterCmd())
	return cmd
}

func catalogRegisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register",
		Short: "Register manifest tables in the configured catalog without re-snapshotting",
		Long: `Reads the existing manifest from storage and registers every active table
in the configured catalog (e.g. AWS Glue).

Use this when you've enabled catalog.type on a deployment that was previously
snapshotted without it — running init again would re-snapshot every table
from Postgres and duplicate the parquet base files in S3.

Safe to re-run: registration is upsert semantics (CreateTable falls through
to UpdateTable on the second attempt).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			sink, err := registry.NewSink(cfg.Storage)
			if err != nil {
				return err
			}
			return internal.CatalogRegister(cmd.Context(), cfg, sink)
		},
	}
}