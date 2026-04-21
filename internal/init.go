// Package internal contains the core pg-cdc command logic.
package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/glue"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/tags"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// Init creates a replication slot, snapshots all discovered tables to base
// Parquet files, and writes a manifest.json. This follows the native
// Postgres replica pattern: snapshot + LSN = zero-gap starting point.
func Init(ctx context.Context, cfg config.Config, source ports.Source, sink ports.Sink) error {
	defer source.Close()

	// Get source version
	pgVersion, _ := source.Version(ctx)
	fmt.Printf("Connected to PostgreSQL: %s\n", truncateVersion(pgVersion))

	// Discover tables
	schemas := cfg.Source.Postgres.Schemas
	if len(schemas) == 0 {
		schemas = []string{"public"}
	}

	var allQualified []string
	type rawTable struct {
		schema string
		name   string
	}
	rawTables := make(map[string]rawTable)

	for _, schema := range schemas {
		tableNames, err := source.ListTables(ctx, schema)
		if err != nil {
			return fmt.Errorf("list tables in %s: %w", schema, err)
		}
		for _, name := range tableNames {
			qualified := schema + "." + name
			allQualified = append(allQualified, qualified)
			rawTables[qualified] = rawTable{schema: schema, name: name}
		}
	}

	// Apply tags + policies
	resolutions := tags.Resolve(allQualified, cfg.Tables)

	excludedTables := make(map[string]tags.Resolution)
	var tables []tableInfo
	for _, res := range resolutions {
		if !res.Included {
			fmt.Printf("  skip: %s (%s)\n", res.Table, res.Reason)
			excludedTables[res.Table] = res
			continue
		}
		rt := rawTables[res.Table]
		pks, err := source.GetPrimaryKeys(ctx, rt.schema, rt.name)
		if err != nil || len(pks) == 0 {
			fmt.Printf("  skip: %s (no primary key)\n", res.Table)
			continue
		}
		cols, err := source.GetTableSchema(ctx, rt.schema, rt.name)
		if err != nil {
			return fmt.Errorf("schema for %s: %w", res.Table, err)
		}
		tables = append(tables, tableInfo{
			schema: rt.schema, name: rt.name, qualified: res.Table,
			pks: pks, cols: cols, tags: res.Tags,
		})
	}

	if len(tables) == 0 {
		return fmt.Errorf("no tables with primary keys found in schemas %v", schemas)
	}
	fmt.Printf("Discovered %d tables\n", len(tables))

	// Create publication
	pubName := cfg.Replication.Publication
	if pubName == "" {
		pubName = "pg_cdc_pub"
	}
	var qualifiedNames []string
	for _, t := range tables {
		qualifiedNames = append(qualifiedNames, t.qualified)
	}
	if err := source.CreatePublication(ctx, pubName, qualifiedNames); err != nil {
		return err
	}
	fmt.Printf("Publication: %s\n", pubName)

	// Create replication slot and get snapshot
	slotName := cfg.Replication.Slot
	if slotName == "" {
		slotName = "pg_cdc_slot"
	}
	snapshot, err := source.CreateReplicationSlot(ctx, slotName)
	if err != nil {
		return err
	}
	fmt.Printf("Replication slot: %s (LSN: %s)\n", slotName, snapshot.LSN)

	// Release the snapshot-holding replication connection after all copies.
	defer source.ReleaseSnapshot(ctx)

	// Snapshot each table using the exported snapshot
	manifestTables := make(map[string]manifest.Table)
	manifestSchemas := make(map[string]manifest.Schema)

	for _, t := range tables {
		fmt.Printf("  snapshot: %s ...", t.qualified)

		rows, _, err := source.CopyTable(ctx, t.schema, t.name, snapshot.SnapshotName)
		if err != nil {
			return fmt.Errorf("copy %s: %w", t.qualified, err)
		}

		// Build manifest columns
		var mCols []manifest.Column
		for _, col := range t.cols {
			mCols = append(mCols, manifest.Column{
				Name:        col.Name,
				PgType:      col.Type,
				ParquetType: btypes.ParquetType(col.Type),
				Nullable:    col.Nullable,
			})
		}

		// Write base Parquet
		basePath := filepath.Join(t.qualified, "base", "base_000000.parquet")
		tmpPath := filepath.Join("/tmp", "pgcdc-init", basePath)
		if err := writer.WriteBaseParquet(tmpPath, rows, mCols); err != nil {
			fmt.Printf(" (parquet write failed: %v, writing raw)\n", err)
		}

		// Upload to storage via sink
		if err := uploadFile(ctx, sink, basePath, tmpPath); err != nil {
			return fmt.Errorf("upload %s: %w", basePath, err)
		}

		fmt.Printf(" %d rows\n", len(rows))

		manifestTables[t.qualified] = manifest.Table{
			Status:           "active",
			Tags:             t.tags,
			PrimaryKey:       t.pks,
			SchemaVersion:    1,
			BaseEpoch:        0,
			LatestDeltaEpoch: 0,
			RowCount:         int64(len(rows)),
			Path:             t.qualified,
		}
		manifestSchemas[t.qualified] = manifest.Schema{
			Version: 1,
			Columns: mCols,
		}
	}

	// Add excluded tables to manifest (with reason)
	for tableName, res := range excludedTables {
		manifestTables[tableName] = manifest.Table{
			Status: "excluded",
			Tags:   res.Tags,
			Reason: res.Reason,
		}
	}

	// Build ACL-driven role profiles
	var roles map[string]manifest.Role
	if cfg.Profiles.Source == "pg_acl" && len(cfg.Profiles.Roles) > 0 {
		fmt.Printf("Reading ACLs for roles: %v\n", cfg.Profiles.Roles)
		portRoles, err := source.BuildRoleProfiles(ctx, cfg.Profiles.Roles)
		if err != nil {
			fmt.Printf("  warning: failed to read ACLs: %v\n", err)
		} else {
			roles = toManifestRoles(portRoles)
			fmt.Printf("  %d role profiles generated\n", len(roles))
		}
	}

	// Write manifest
	m := &manifest.Manifest{
		Version: 1,
		Source: manifest.Source{
			Name:            slotName,
			PostgresVersion: truncateVersion(pgVersion),
			InitLSN:         snapshot.LSN,
			InitTimestamp:    time.Now().UTC(),
		},
		Tables:    manifestTables,
		Schemas:   manifestSchemas,
		Roles:     roles,
		UpdatedAt: time.Now().UTC(),
	}

	if err := sink.WriteManifest(ctx, m); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	if err := registerCatalog(ctx, cfg, m); err != nil {
		return fmt.Errorf("catalog register: %w", err)
	}

	fmt.Printf("\nInit complete:\n")
	fmt.Printf("  Slot: %s\n", slotName)
	fmt.Printf("  LSN: %s\n", snapshot.LSN)
	fmt.Printf("  Tables: %d\n", len(tables))
	fmt.Printf("  Epoch: %s\n", epoch.FormatFilename(0))
	fmt.Printf("  Storage: %s (%s)\n", cfg.Storage.Path, sink.Backend())
	return nil
}

// CatalogRegister reads the manifest from sink and registers every active
// table in the configured catalog (e.g. AWS Glue). Use this to turn on
// catalog.type for a deployment that was already snapshotted without that
// block — re-running `init` would re-snapshot from Postgres and duplicate
// the parquet base files in storage.
//
// Stricter than the init-time call: returns an error if catalog.type is
// empty, since a no-op invocation is almost certainly an operator mistake.
func CatalogRegister(ctx context.Context, cfg config.Config, sink ports.Sink) error {
	if cfg.Catalog.Type == "" {
		return fmt.Errorf("catalog.type is not set in config; nothing to register")
	}
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest: %w (run `pg-cdc init` first if this deployment has never been snapshotted)", err)
	}
	return registerCatalog(ctx, cfg, m)
}

// registerCatalog catalogues active manifest tables when cfg.Catalog.Type is
// set. Today only "glue" is supported, and only on the s3 sink — other
// backends are skipped with a notice. Failure surfaces as an error; the
// manifest is already on disk, so re-running init is the recovery path
// (CreateTable falls through to UpdateTable on the second attempt).
func registerCatalog(ctx context.Context, cfg config.Config, m *manifest.Manifest) error {
	if cfg.Catalog.Type == "" {
		return nil
	}
	if cfg.Catalog.Type != "glue" {
		return fmt.Errorf("unsupported catalog type %q", cfg.Catalog.Type)
	}
	if cfg.Storage.StorageType() != "s3" {
		fmt.Printf("  catalog: skipping Glue registration (storage type %q is not s3)\n", cfg.Storage.StorageType())
		return nil
	}

	region := cfg.Catalog.Region
	if region == "" {
		region = cfg.Storage.Region
	}
	g, err := glue.New(glue.Config{
		Database: cfg.Catalog.Database,
		Region:   region,
		Endpoint: cfg.Catalog.Endpoint,
	})
	if err != nil {
		return err
	}

	locFn := func(qualified string) string {
		return fmt.Sprintf("s3://%s/%s%s/", cfg.Storage.Bucket, cfg.Storage.Prefix, qualified)
	}
	if err := g.Register(ctx, m, locFn); err != nil {
		return err
	}

	active := 0
	for _, t := range m.Tables {
		if t.Status == "active" {
			active++
		}
	}
	fmt.Printf("  Glue catalog: %s (%d tables)\n", g.Database(), active)
	return nil
}

func uploadFile(ctx context.Context, sink ports.Sink, remotePath, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return sink.WriteFile(ctx, remotePath, f)
}

type tableInfo struct {
	schema    string
	name      string
	qualified string
	pks       []string
	cols      []ports.ColumnInfo
	tags      []string
}

func truncateVersion(v string) string {
	if idx := strings.IndexByte(v, '('); idx > 0 {
		return strings.TrimSpace(v[:idx])
	}
	return v
}

// toManifestRoles converts port-level role profiles to manifest types.
func toManifestRoles(portRoles map[string]ports.RoleProfile) map[string]manifest.Role {
	roles := make(map[string]manifest.Role, len(portRoles))
	for name, profile := range portRoles {
		role := manifest.Role{Tables: make(map[string]manifest.RoleAccess, len(profile.Tables))}
		for tbl, access := range profile.Tables {
			role.Tables[tbl] = manifest.RoleAccess{
				AllColumns: access.AllColumns,
				Columns:    access.Columns,
			}
		}
		roles[name] = role
	}
	return roles
}
