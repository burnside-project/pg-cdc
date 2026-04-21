// Package ports defines the hexagonal architecture boundaries for pg-cdc.
// Ports are interfaces — adapters implement them for specific technologies.
package ports

import (
	"context"
	"time"
)

// ColumnInfo describes a table column from the source database.
type ColumnInfo struct {
	Name     string
	Type     string // source-native type (e.g., PostgreSQL data_type)
	Nullable bool
	Position int
}

// CDCEvent represents a single change captured from the source WAL.
type CDCEvent struct {
	Table     string         // qualified table name (schema.table)
	Op        string         // "I" (insert), "U" (update), "D" (delete)
	Row       map[string]any // column values
	LSN       string         // log sequence number (source-specific)
	Timestamp time.Time
}

// Snapshot represents an exported transaction snapshot for consistent reads.
type Snapshot struct {
	LSN          string // consistent point LSN
	SnapshotName string // snapshot identifier for SET TRANSACTION SNAPSHOT
}

// Source is the inbound port for reading from a database.
// The only implementation today is PostgreSQL; the interface exists so the
// CDC core never imports pgx, pglogrepl, or any driver directly.
type Source interface {
	// Discovery
	ListTables(ctx context.Context, schema string) ([]string, error)
	GetTableSchema(ctx context.Context, schema, table string) ([]ColumnInfo, error)
	GetPrimaryKeys(ctx context.Context, schema, table string) ([]string, error)
	Version(ctx context.Context) (string, error)

	// Snapshot
	CreateReplicationSlot(ctx context.Context, slotName string) (Snapshot, error)
	ReleaseSnapshot(ctx context.Context)
	CreatePublication(ctx context.Context, pubName string, tables []string) error
	CopyTable(ctx context.Context, schema, table, snapshotName string) ([]map[string]any, []ColumnInfo, error)

	// Streaming
	Stream(ctx context.Context, slotName, pubName, startLSN string, events chan<- CDCEvent) error

	// Governance
	BuildRoleProfiles(ctx context.Context, roles []string) (map[string]RoleProfile, error)

	// Teardown
	DropReplicationSlot(ctx context.Context, slotName string) error
	DropPublication(ctx context.Context, pubName string) error

	// Health
	QueryRow(ctx context.Context, sql string, args ...any) Row

	// Lifecycle
	Close()
}

// Row is a minimal interface for scanning a single database row.
type Row interface {
	Scan(dest ...any) error
}

// RoleProfile describes table-level and column-level access for a database role.
type RoleProfile struct {
	Tables map[string]RoleAccess
}

// RoleAccess describes access to a single table for a role.
type RoleAccess struct {
	AllColumns bool
	Columns    []string
}