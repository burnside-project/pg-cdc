// Package postgres adapts internal/postgres into a ports.Source implementation.
package postgres

import (
	"context"

	pg "github.com/burnside-project/pg-cdc-core/internal/postgres"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// Adapter wraps the concrete postgres.Conn behind ports.Source.
type Adapter struct {
	conn *pg.Conn
}

// New creates a PostgreSQL source adapter.
func New(ctx context.Context, url string) (*Adapter, error) {
	conn, err := pg.NewConn(ctx, url)
	if err != nil {
		return nil, err
	}
	return &Adapter{conn: conn}, nil
}

func (a *Adapter) ListTables(ctx context.Context, schema string) ([]string, error) {
	return a.conn.ListTables(ctx, schema)
}

func (a *Adapter) GetTableSchema(ctx context.Context, schema, table string) ([]ports.ColumnInfo, error) {
	cols, err := a.conn.GetTableSchema(ctx, schema, table)
	if err != nil {
		return nil, err
	}
	out := make([]ports.ColumnInfo, len(cols))
	for i, c := range cols {
		out[i] = ports.ColumnInfo{Name: c.Name, Type: c.Type, Nullable: c.Nullable, Position: c.Position}
	}
	return out, nil
}

func (a *Adapter) GetPrimaryKeys(ctx context.Context, schema, table string) ([]string, error) {
	return a.conn.GetPrimaryKeys(ctx, schema, table)
}

func (a *Adapter) Version(ctx context.Context) (string, error) {
	return a.conn.PostgresVersion(ctx)
}

func (a *Adapter) CreateReplicationSlot(ctx context.Context, slotName string) (ports.Snapshot, error) {
	lsn, snap, err := a.conn.CreateReplicationSlot(ctx, slotName)
	if err != nil {
		return ports.Snapshot{}, err
	}
	return ports.Snapshot{LSN: lsn, SnapshotName: snap}, nil
}

func (a *Adapter) ReleaseSnapshot(ctx context.Context) {
	a.conn.ReleaseSnapshot(ctx)
}

func (a *Adapter) CreatePublication(ctx context.Context, pubName string, tables []string) error {
	return a.conn.CreatePublication(ctx, pubName, tables)
}

func (a *Adapter) CopyTable(ctx context.Context, schema, table, snapshotName string) ([]map[string]any, []ports.ColumnInfo, error) {
	rows, cols, err := a.conn.CopyTable(ctx, schema, table, snapshotName)
	if err != nil {
		return nil, nil, err
	}
	out := make([]ports.ColumnInfo, len(cols))
	for i, c := range cols {
		out[i] = ports.ColumnInfo{Name: c.Name, Type: c.Type, Nullable: c.Nullable, Position: c.Position}
	}
	return rows, out, nil
}

func (a *Adapter) Stream(ctx context.Context, slotName, pubName, startLSN string, events chan<- ports.CDCEvent) error {
	// Bridge pg-specific CDCEvent to ports.CDCEvent via an intermediate channel.
	pgEvents := make(chan pg.CDCEvent, cap(events))
	errCh := make(chan error, 1)

	go func() {
		errCh <- a.conn.Stream(ctx, slotName, pubName, startLSN, pgEvents)
	}()

	for evt := range pgEvents {
		events <- ports.CDCEvent{
			Table:     evt.Table,
			Op:        evt.Op,
			Row:       evt.Row,
			LSN:       evt.LSN,
			Timestamp: evt.Timestamp,
		}
	}
	close(events)
	return <-errCh
}

func (a *Adapter) BuildRoleProfiles(ctx context.Context, roles []string) (map[string]ports.RoleProfile, error) {
	mRoles, err := a.conn.BuildRoleProfiles(ctx, roles)
	if err != nil {
		return nil, err
	}
	out := make(map[string]ports.RoleProfile, len(mRoles))
	for name, role := range mRoles {
		profile := ports.RoleProfile{Tables: make(map[string]ports.RoleAccess, len(role.Tables))}
		for tbl, access := range role.Tables {
			profile.Tables[tbl] = ports.RoleAccess{
				AllColumns: access.AllColumns,
				Columns:    access.Columns,
			}
		}
		out[name] = profile
	}
	return out, nil
}

func (a *Adapter) DropReplicationSlot(ctx context.Context, slotName string) error {
	return a.conn.DropReplicationSlot(ctx, slotName)
}

func (a *Adapter) DropPublication(ctx context.Context, pubName string) error {
	return a.conn.DropPublication(ctx, pubName)
}

func (a *Adapter) QueryRow(ctx context.Context, sql string, args ...any) ports.Row {
	return a.conn.QueryRow(ctx, sql, args...)
}

func (a *Adapter) Close() {
	a.conn.Close()
}