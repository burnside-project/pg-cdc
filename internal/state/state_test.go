package state

import (
	"context"
	"path/filepath"
	"testing"
)

func TestOpenAndMigrate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()
}

func TestOpenCreatesParentDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "dir", "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open with nested dir: %v", err)
	}
	defer func() { _ = s.Close() }()
}

func TestLockStateTable(t *testing.T) {
	s := openTestDB(t)
	// lock_state table should exist after migration — the systemd
	// ExecStartPre runs "DELETE FROM lock_state" before starting.
	_, err := s.db.Exec("DELETE FROM lock_state")
	if err != nil {
		t.Fatalf("lock_state table should exist: %v", err)
	}
}

func TestReplicationState(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	// Initially nil
	rs, err := s.GetReplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rs != nil {
		t.Fatal("expected nil")
	}

	// Save
	err = s.SaveReplicationState(ctx, &ReplicationState{
		SlotName:        "pg_cdc_slot",
		PublicationName: "pg_cdc_pub",
		ConfirmedLSN:    "0/16B3740",
		Status:          "streaming",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read back
	rs, err = s.GetReplicationState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rs.ConfirmedLSN != "0/16B3740" {
		t.Errorf("LSN = %q", rs.ConfirmedLSN)
	}
	if rs.Status != "streaming" {
		t.Errorf("status = %q", rs.Status)
	}

	// Update
	rs.ConfirmedLSN = "0/16B4000"
	rs.Status = "stopped"
	if err := s.SaveReplicationState(ctx, rs); err != nil {
		t.Fatal(err)
	}

	rs2, _ := s.GetReplicationState(ctx)
	if rs2.ConfirmedLSN != "0/16B4000" {
		t.Errorf("updated LSN = %q", rs2.ConfirmedLSN)
	}
}

func TestEpochState(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	// No epochs initially
	latest, err := s.GetLatestEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if latest != nil {
		t.Fatal("expected nil")
	}

	// Open epoch
	ep, err := s.OpenEpoch(ctx, 1, "0/1000")
	if err != nil {
		t.Fatal(err)
	}
	if ep.EpochNumber != 1 {
		t.Errorf("epoch = %d", ep.EpochNumber)
	}
	if ep.Status != "open" {
		t.Errorf("status = %q", ep.Status)
	}

	// Commit
	if err := s.CommitEpoch(ctx, ep.ID, "0/2000", 42); err != nil {
		t.Fatal(err)
	}

	// Get latest
	latest, _ = s.GetLatestEpoch(ctx)
	if latest.Status != "committed" {
		t.Errorf("status = %q", latest.Status)
	}
	if latest.RowCount != 42 {
		t.Errorf("row_count = %d", latest.RowCount)
	}
}

func TestTableState(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	// Save
	err := s.SaveTableState(ctx, &TableState{
		TableName:       "public.orders",
		LastSnapshotLSN: "0/1000",
		RowCount:        100,
		SchemaVersion:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Read
	ts, err := s.GetTableState(ctx, "public.orders")
	if err != nil {
		t.Fatal(err)
	}
	if ts.RowCount != 100 {
		t.Errorf("row_count = %d", ts.RowCount)
	}

	// Not found
	ts2, _ := s.GetTableState(ctx, "nonexistent")
	if ts2 != nil {
		t.Error("expected nil for nonexistent")
	}
}

func TestCompactionState(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	err := s.SaveCompactionState(ctx, &CompactionStateRecord{
		TableName:      "public.orders",
		LastBaseEpoch:  5,
		TombstoneCount: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func openTestDB(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
