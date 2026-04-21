package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/burnside-project/pg-cdc-core/internal"
	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/registry"
	"github.com/burnside-project/pg-cdc-core/pkg/version"
	"github.com/spf13/cobra"
)

var cfgFile string

func main() {
	root := &cobra.Command{
		Use:   "pg-cdc",
		Short: "PostgreSQL CDC server — WAL streaming to typed Parquet",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}

	root.PersistentFlags().StringVar(&cfgFile, "config", "pg-cdc.yml", "config file path")

	root.AddCommand(initCmd())
	root.AddCommand(startCmd())
	root.AddCommand(compactCmd())
	root.AddCommand(statusCmd())
	root.AddCommand(discoverCmd())
	root.AddCommand(teardownCmd())
	root.AddCommand(catalogCmd())
	root.AddCommand(versionCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func initCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Create replication slot and snapshot tables to Parquet",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			source, err := registry.NewSource(ctx, cfg.Source)
			if err != nil {
				return err
			}
			sink, err := registry.NewSink(cfg.Storage)
			if err != nil {
				return err
			}
			return internal.Init(ctx, cfg, source, sink)
		},
	}
}

func startCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Stream WAL changes to delta Parquet files",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			source, err := registry.NewSource(ctx, cfg.Source)
			if err != nil {
				return err
			}
			sink, err := registry.NewSink(cfg.Storage)
			if err != nil {
				return err
			}
			return internal.Start(ctx, cfg, source, sink)
		},
	}
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show replication health: slot, LSN, epochs, tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			source, err := registry.NewSource(ctx, cfg.Source)
			if err != nil {
				return err
			}
			sink, err := registry.NewSink(cfg.Storage)
			if err != nil {
				return err
			}
			return internal.Status(ctx, cfg, source, sink)
		},
	}
}

func teardownCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "teardown",
		Short: "Drop publication and replication slot",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			source, err := registry.NewSource(ctx, cfg.Source)
			if err != nil {
				return err
			}
			return internal.Teardown(ctx, cfg, source)
		},
	}
}

func discoverCmd() *cobra.Command {
	var showACL bool
	var dryRun bool

	cmd := &cobra.Command{
		Use:   "discover",
		Short: "List tables with tags, policies, and optionally ACL profiles",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			source, err := registry.NewSource(ctx, cfg.Source)
			if err != nil {
				return err
			}
			return internal.Discover(ctx, cfg, source, showACL, dryRun)
		},
	}

	cmd.Flags().BoolVar(&showACL, "acl", false, "Show role → table → column access from PostgreSQL ACLs")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview only, no changes")
	return cmd
}

func compactCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "compact",
		Short: "Merge delta files into a new base snapshot",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return err
			}
			sink, err := registry.NewSink(cfg.Storage)
			if err != nil {
				return err
			}
			return internal.Compact(cmd.Context(), sink)
		},
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("pg-cdc %s\n", version.Version)
		},
	}
}

func loadConfig() (config.Config, error) {
	data, err := os.ReadFile(cfgFile)
	if err != nil {
		return config.Config{}, fmt.Errorf("read config %s: %w", cfgFile, err)
	}
	var cfg config.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return config.Config{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}
