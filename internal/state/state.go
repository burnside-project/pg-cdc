// Package state provides a SQLite-backed state store for pg-cdc.
// Tracks replication LSN, epoch lifecycle, per-table metadata, and compaction progress.
// Crash-safe: atomic writes independent of the storage backend (S3/GCS/filesystem).
package state

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps a SQLite database for pg-cdc state management.
type Store struct {
	db *sql.DB
}

// Open creates or opens a SQLite state database at the given path.
// Creates the parent directory if it does not exist.
func Open(path string) (*Store, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open state db: %w", err)
	}
	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate state db: %w", err)
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS replication_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			slot_name TEXT NOT NULL,
			publication_name TEXT NOT NULL,
			confirmed_lsn TEXT NOT NULL,
			last_received_lsn TEXT,
			status TEXT NOT NULL DEFAULT 'stopped',
			error_message TEXT,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS epoch_state (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			epoch_number INTEGER NOT NULL,
			started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			committed_at TIMESTAMP,
			start_lsn TEXT,
			end_lsn TEXT,
			row_count INTEGER DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'open'
		)`,
		`CREATE TABLE IF NOT EXISTS table_state (
			table_name TEXT PRIMARY KEY,
			last_snapshot_lsn TEXT,
			row_count INTEGER DEFAULT 0,
			schema_version INTEGER DEFAULT 1,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS compaction_state (
			table_name TEXT PRIMARY KEY,
			last_base_epoch INTEGER DEFAULT 0,
			last_compacted_at TIMESTAMP,
			tombstone_count INTEGER DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS lock_state (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			holder TEXT NOT NULL,
			acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}
	for _, m := range migrations {
		if _, err := s.db.Exec(m); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}
	return nil
}

// --- Replication State ---

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

// GetReplicationState returns the current replication state, or nil if not initialized.
func (s *Store) GetReplicationState(ctx context.Context) (*ReplicationState, error) {
	var rs ReplicationState
	err := s.db.QueryRowContext(ctx,
		`SELECT slot_name, publication_name, confirmed_lsn, COALESCE(last_received_lsn, ''),
		        status, COALESCE(error_message, ''), updated_at
		 FROM replication_state WHERE id = 1`).
		Scan(&rs.SlotName, &rs.PublicationName, &rs.ConfirmedLSN, &rs.LastReceivedLSN,
			&rs.Status, &rs.ErrorMessage, &rs.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rs, nil
}

// SaveReplicationState upserts the replication state.
func (s *Store) SaveReplicationState(ctx context.Context, rs *ReplicationState) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO replication_state (id, slot_name, publication_name, confirmed_lsn, last_received_lsn, status, error_message, updated_at)
		 VALUES (1, ?, ?, ?, ?, ?, ?, datetime('now'))
		 ON CONFLICT(id) DO UPDATE SET
		   slot_name = excluded.slot_name,
		   publication_name = excluded.publication_name,
		   confirmed_lsn = excluded.confirmed_lsn,
		   last_received_lsn = excluded.last_received_lsn,
		   status = excluded.status,
		   error_message = excluded.error_message,
		   updated_at = datetime('now')`,
		rs.SlotName, rs.PublicationName, rs.ConfirmedLSN, rs.LastReceivedLSN, rs.Status, rs.ErrorMessage)
	return err
}

// --- Epoch State ---

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

// OpenEpoch creates a new epoch and returns it.
func (s *Store) OpenEpoch(ctx context.Context, epochNumber int64, startLSN string) (*EpochState, error) {
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO epoch_state (epoch_number, start_lsn, status) VALUES (?, ?, 'open')`,
		epochNumber, startLSN)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return &EpochState{
		ID:          id,
		EpochNumber: epochNumber,
		StartLSN:    startLSN,
		Status:      "open",
		StartedAt:   time.Now(),
	}, nil
}

// CommitEpoch marks an epoch as committed.
func (s *Store) CommitEpoch(ctx context.Context, id int64, endLSN string, rowCount int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE epoch_state SET status = 'committed', end_lsn = ?, row_count = ?, committed_at = datetime('now') WHERE id = ?`,
		endLSN, rowCount, id)
	return err
}

// GetLatestEpoch returns the most recent epoch by number.
func (s *Store) GetLatestEpoch(ctx context.Context) (*EpochState, error) {
	var es EpochState
	var committedAt sql.NullTime
	err := s.db.QueryRowContext(ctx,
		`SELECT id, epoch_number, started_at, committed_at, COALESCE(start_lsn, ''), COALESCE(end_lsn, ''), row_count, status
		 FROM epoch_state ORDER BY epoch_number DESC LIMIT 1`).
		Scan(&es.ID, &es.EpochNumber, &es.StartedAt, &committedAt, &es.StartLSN, &es.EndLSN, &es.RowCount, &es.Status)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if committedAt.Valid {
		es.CommittedAt = &committedAt.Time
	}
	return &es, nil
}

// --- Table State ---

// TableState holds per-table metadata.
type TableState struct {
	TableName      string
	LastSnapshotLSN string
	RowCount       int64
	SchemaVersion  int
	UpdatedAt      time.Time
}

// SaveTableState upserts per-table state.
func (s *Store) SaveTableState(ctx context.Context, ts *TableState) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO table_state (table_name, last_snapshot_lsn, row_count, schema_version, updated_at)
		 VALUES (?, ?, ?, ?, datetime('now'))
		 ON CONFLICT(table_name) DO UPDATE SET
		   last_snapshot_lsn = excluded.last_snapshot_lsn,
		   row_count = excluded.row_count,
		   schema_version = excluded.schema_version,
		   updated_at = datetime('now')`,
		ts.TableName, ts.LastSnapshotLSN, ts.RowCount, ts.SchemaVersion)
	return err
}

// GetTableState returns state for a specific table.
func (s *Store) GetTableState(ctx context.Context, tableName string) (*TableState, error) {
	var ts TableState
	err := s.db.QueryRowContext(ctx,
		`SELECT table_name, COALESCE(last_snapshot_lsn, ''), row_count, schema_version, updated_at
		 FROM table_state WHERE table_name = ?`, tableName).
		Scan(&ts.TableName, &ts.LastSnapshotLSN, &ts.RowCount, &ts.SchemaVersion, &ts.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &ts, nil
}

// --- Compaction State ---

// CompactionState holds per-table compaction progress.
type CompactionStateRecord struct {
	TableName       string
	LastBaseEpoch   int64
	LastCompactedAt *time.Time
	TombstoneCount  int64
}

// SaveCompactionState upserts compaction state.
func (s *Store) SaveCompactionState(ctx context.Context, cs *CompactionStateRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO compaction_state (table_name, last_base_epoch, last_compacted_at, tombstone_count)
		 VALUES (?, ?, datetime('now'), ?)
		 ON CONFLICT(table_name) DO UPDATE SET
		   last_base_epoch = excluded.last_base_epoch,
		   last_compacted_at = datetime('now'),
		   tombstone_count = excluded.tombstone_count`,
		cs.TableName, cs.LastBaseEpoch, cs.TombstoneCount)
	return err
}
