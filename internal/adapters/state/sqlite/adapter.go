// Package sqlite adapts internal/state as a ports.StateStore implementation.
package sqlite

import (
	"context"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/state"
)

// Adapter wraps the concrete state.Store behind ports.StateStore.
type Adapter struct {
	store *state.Store
}

// Verify interface compliance at compile time.
var _ ports.StateStore = (*Adapter)(nil)

// New opens a SQLite state store at the given path.
func New(path string) (*Adapter, error) {
	s, err := state.Open(path)
	if err != nil {
		return nil, err
	}
	return &Adapter{store: s}, nil
}

func (a *Adapter) GetReplicationState(ctx context.Context) (*ports.ReplicationState, error) {
	rs, err := a.store.GetReplicationState(ctx)
	if err != nil || rs == nil {
		return nil, err
	}
	return &ports.ReplicationState{
		SlotName:        rs.SlotName,
		PublicationName: rs.PublicationName,
		ConfirmedLSN:    rs.ConfirmedLSN,
		LastReceivedLSN: rs.LastReceivedLSN,
		Status:          rs.Status,
		ErrorMessage:    rs.ErrorMessage,
		UpdatedAt:       rs.UpdatedAt,
	}, nil
}

func (a *Adapter) SaveReplicationState(ctx context.Context, rs *ports.ReplicationState) error {
	return a.store.SaveReplicationState(ctx, &state.ReplicationState{
		SlotName:        rs.SlotName,
		PublicationName: rs.PublicationName,
		ConfirmedLSN:    rs.ConfirmedLSN,
		LastReceivedLSN: rs.LastReceivedLSN,
		Status:          rs.Status,
		ErrorMessage:    rs.ErrorMessage,
	})
}

func (a *Adapter) OpenEpoch(ctx context.Context, epochNumber int64, startLSN string) (*ports.EpochState, error) {
	es, err := a.store.OpenEpoch(ctx, epochNumber, startLSN)
	if err != nil {
		return nil, err
	}
	return &ports.EpochState{
		ID:          es.ID,
		EpochNumber: es.EpochNumber,
		StartedAt:   es.StartedAt,
		CommittedAt: es.CommittedAt,
		StartLSN:    es.StartLSN,
		EndLSN:      es.EndLSN,
		RowCount:    es.RowCount,
		Status:      es.Status,
	}, nil
}

func (a *Adapter) CommitEpoch(ctx context.Context, id int64, endLSN string, rowCount int64) error {
	return a.store.CommitEpoch(ctx, id, endLSN, rowCount)
}

func (a *Adapter) GetLatestEpoch(ctx context.Context) (*ports.EpochState, error) {
	es, err := a.store.GetLatestEpoch(ctx)
	if err != nil || es == nil {
		return nil, err
	}
	return &ports.EpochState{
		ID:          es.ID,
		EpochNumber: es.EpochNumber,
		StartedAt:   es.StartedAt,
		CommittedAt: es.CommittedAt,
		StartLSN:    es.StartLSN,
		EndLSN:      es.EndLSN,
		RowCount:    es.RowCount,
		Status:      es.Status,
	}, nil
}

func (a *Adapter) SaveTableState(ctx context.Context, ts *ports.TableState) error {
	return a.store.SaveTableState(ctx, &state.TableState{
		TableName:       ts.TableName,
		LastSnapshotLSN: ts.LastSnapshotLSN,
		RowCount:        ts.RowCount,
		SchemaVersion:   ts.SchemaVersion,
	})
}

func (a *Adapter) GetTableState(ctx context.Context, tableName string) (*ports.TableState, error) {
	ts, err := a.store.GetTableState(ctx, tableName)
	if err != nil || ts == nil {
		return nil, err
	}
	return &ports.TableState{
		TableName:       ts.TableName,
		LastSnapshotLSN: ts.LastSnapshotLSN,
		RowCount:        ts.RowCount,
		SchemaVersion:   ts.SchemaVersion,
		UpdatedAt:       ts.UpdatedAt,
	}, nil
}

func (a *Adapter) SaveCompactionState(ctx context.Context, cs *ports.CompactionStateRecord) error {
	return a.store.SaveCompactionState(ctx, &state.CompactionStateRecord{
		TableName:      cs.TableName,
		LastBaseEpoch:  cs.LastBaseEpoch,
		TombstoneCount: cs.TombstoneCount,
	})
}

func (a *Adapter) Close() error {
	return a.store.Close()
}