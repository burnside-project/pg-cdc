// Package registry maps configuration to concrete adapter instances.
// This is the only package that imports adapter implementations directly.
package registry

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/gcs"
	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/s3"
	pgadapter "github.com/burnside-project/pg-cdc-core/internal/adapters/source/postgres"
	sqliteadapter "github.com/burnside-project/pg-cdc-core/internal/adapters/state/sqlite"
	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// NewSource creates a Source adapter from config.
// Currently only PostgreSQL is supported.
func NewSource(ctx context.Context, cfg config.SourceConfig) (ports.Source, error) {
	return pgadapter.New(ctx, cfg.Postgres.URL)
}

// NewSink creates a Sink adapter from config.
func NewSink(cfg config.StorageConfig) (ports.Sink, error) {
	switch cfg.StorageType() {
	case "filesystem":
		if cfg.Path == "" {
			return nil, fmt.Errorf("storage.path is required for filesystem backend")
		}
		return filesystem.New(cfg.Path)

	case "gcs":
		return gcs.New(gcs.Config{
			Bucket:          cfg.Bucket,
			Prefix:          cfg.Prefix,
			CredentialsFile: cfg.CredentialsFile,
		})

	case "s3":
		return s3.New(s3.Config{
			Bucket:   cfg.Bucket,
			Prefix:   cfg.Prefix,
			Region:   cfg.Region,
			Endpoint: cfg.Endpoint,
		})

	default:
		return nil, fmt.Errorf("unknown storage type: %q (supported: filesystem, gcs, s3)", cfg.StorageType())
	}
}

// NewStateStore creates a StateStore adapter from config.
func NewStateStore(cfg config.StateConfig) (ports.StateStore, error) {
	return sqliteadapter.New(cfg.StatePath())
}
