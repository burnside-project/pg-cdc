package ports

import (
	"context"
	"time"
)

// StateStore is the outbound port for persisting replication progress.
// The default adapter is SQLite; the interface allows alternative backends.
type StateStore interface {
	// Replication state
	GetReplicationState(ctx context.Context) (*ReplicationState, error)
	SaveReplicationState(ctx context.Context, rs *ReplicationState) error

	// Epoch lifecycle
	OpenEpoch(ctx context.Context, epochNumber int64, startLSN string) (*EpochState, error)
	CommitEpoch(ctx context.Context, id int64, endLSN string, rowCount int64) error
	GetLatestEpoch(ctx context.Context) (*EpochState, error)

	// Per-table state
	SaveTableState(ctx context.Context, ts *TableState) error
	GetTableState(ctx context.Context, tableName string) (*TableState, error)

	// Compaction state
	SaveCompactionState(ctx context.Context, cs *CompactionStateRecord) error

	// Lifecycle
	Close() error
}

// ReplicationState holds the current replication slot state.
type ReplicationState struct {
	SlotName        string
	PublicationName string
	ConfirmedLSN    string
	LastReceivedLSN string
	Status          string
	ErrorMessage    string
	UpdatedAt       time.Time
}

// EpochState holds the state of a CDC epoch.
type EpochState struct {
	ID          int64
	EpochNumber int64
	StartedAt   time.Time
	CommittedAt *time.Time
	StartLSN    string
	EndLSN      string
	RowCount    int64
	Status      string // "open", "committed", "flushed"
}

// TableState holds per-table metadata.
type TableState struct {
	TableName       string
	LastSnapshotLSN string
	RowCount        int64
	SchemaVersion   int
	UpdatedAt       time.Time
}

// CompactionStateRecord holds per-table compaction progress.
type CompactionStateRecord struct {
	TableName       string
	LastBaseEpoch   int64
	LastCompactedAt *time.Time
	TombstoneCount  int64
}