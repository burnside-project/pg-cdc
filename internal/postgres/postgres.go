// Package postgres provides PostgreSQL connectivity for CDC:
// table discovery, schema introspection, replication slot management, and COPY.
package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Conn wraps a pgxpool for SQL operations and provides replication helpers.
type Conn struct {
	pool     *pgxpool.Pool
	url      string
	replConn *pgconn.PgConn // held open during snapshot to keep it valid
}

// NewConn creates a connection pool to PostgreSQL.
// The URL supports all pgx/v5 connection parameters including TLS:
//   - sslmode=verify-full (mTLS, AWS RDS)
//   - sslcert, sslkey, sslrootcert (client certificate paths)
//   - password via URL or environment variable substitution
func NewConn(ctx context.Context, url string) (*Conn, error) {
	// Expand environment variables in the URL (e.g., ${RDS_TOKEN})
	url = os.ExpandEnv(url)

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	cfg.MaxConns = 3
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Conn{pool: pool, url: url}, nil
}

// Close releases the connection pool and any held replication connection.
func (c *Conn) Close() {
	if c.replConn != nil {
		_ = c.replConn.Close(context.Background())
		c.replConn = nil
	}
	c.pool.Close()
}

// QueryRow executes a query that returns a single row.
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.pool.QueryRow(ctx, sql, args...)
}

// ColumnInfo describes a table column.
type ColumnInfo struct {
	Name     string
	Type     string // PostgreSQL data_type
	Nullable bool
	Position int
}

// ListTables returns all base table names in a schema.
func (c *Conn) ListTables(ctx context.Context, schema string) ([]string, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name`,
		schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// GetPrimaryKeys returns primary key column names for a table.
func (c *Conn) GetPrimaryKeys(ctx context.Context, schema, table string) ([]string, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT a.attname FROM pg_index i
		 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		 WHERE i.indrelid = ($1 || '.' || $2)::regclass AND i.indisprimary
		 ORDER BY array_position(i.indkey, a.attnum)`,
		schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		keys = append(keys, col)
	}
	return keys, rows.Err()
}

// GetTableSchema returns column metadata for a table.
func (c *Conn) GetTableSchema(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT column_name, data_type, is_nullable, ordinal_position
		 FROM information_schema.columns
		 WHERE table_schema = $1 AND table_name = $2
		 ORDER BY ordinal_position`,
		schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &col.Position); err != nil {
			return nil, err
		}
		col.Nullable = nullable == "YES"
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// CreateReplicationSlot creates a logical replication slot and returns the
// consistent LSN and snapshot name for use with SET TRANSACTION SNAPSHOT.
func (c *Conn) CreateReplicationSlot(ctx context.Context, slotName string) (lsn string, snapshotName string, err error) {
	replConn, err := c.connectReplication(ctx)
	if err != nil {
		return "", "", fmt.Errorf("replication connect: %w", err)
	}

	result, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
			Mode:      pglogrepl.LogicalReplication,
		})
	if err != nil {
		_ = replConn.Close(ctx)
		if strings.Contains(err.Error(), "already exists") {
			return "", "", fmt.Errorf("replication slot %q already exists — run teardown first", slotName)
		}
		return "", "", fmt.Errorf("create replication slot: %w", err)
	}

	// Keep the replication connection open — the exported snapshot is only
	// valid for the lifetime of the session that created the slot.
	c.replConn = replConn

	return result.ConsistentPoint, result.SnapshotName, nil
}

// ReleaseSnapshot closes the replication connection that holds the exported
// snapshot open. Call this after all CopyTable calls are complete.
func (c *Conn) ReleaseSnapshot(ctx context.Context) {
	if c.replConn != nil {
		_ = c.replConn.Close(ctx)
		c.replConn = nil
	}
}

// CreatePublication creates a publication for the given tables.
func (c *Conn) CreatePublication(ctx context.Context, pubName string, tables []string) error {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	var tableList []string
	for _, t := range tables {
		parts := strings.SplitN(t, ".", 2)
		if len(parts) == 2 {
			tableList = append(tableList, pgx.Identifier{parts[0], parts[1]}.Sanitize())
		} else {
			tableList = append(tableList, pgx.Identifier{t}.Sanitize())
		}
	}

	sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		pgx.Identifier{pubName}.Sanitize(), strings.Join(tableList, ", "))

	if _, err := conn.Exec(ctx, sql); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("create publication: %w", err)
	}
	return nil
}

// CopyTable streams all rows from a table using COPY TO STDOUT.
// Returns rows as []map[string]any and the column metadata.
func (c *Conn) CopyTable(ctx context.Context, schema, table, snapshotName string) ([]map[string]any, []ColumnInfo, error) {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	// Use the exported snapshot for consistency
	if snapshotName != "" {
		if _, err := conn.Exec(ctx, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			return nil, nil, fmt.Errorf("begin transaction: %w", err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
			return nil, nil, fmt.Errorf("set snapshot: %w", err)
		}
	}

	// Get schema first
	cols, err := c.GetTableSchema(ctx, schema, table)
	if err != nil {
		return nil, nil, err
	}

	// Fetch rows
	qualifiedTable := pgx.Identifier{schema, table}.Sanitize()
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s", qualifiedTable))
	if err != nil {
		return nil, nil, fmt.Errorf("query table: %w", err)
	}
	defer rows.Close()

	fieldDescs := rows.FieldDescriptions()
	colNames := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		colNames[i] = fd.Name
	}

	var result []map[string]any
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, fmt.Errorf("scan row: %w", err)
		}
		row := make(map[string]any, len(colNames))
		for i, name := range colNames {
			row[name] = values[i]
		}
		result = append(result, row)
	}

	if snapshotName != "" {
		_, _ = conn.Exec(ctx, "COMMIT")
	}

	return result, cols, rows.Err()
}

// DropReplicationSlot drops a replication slot.
func (c *Conn) DropReplicationSlot(ctx context.Context, slotName string) error {
	replConn, err := c.connectReplication(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = replConn.Close(ctx) }()

	return pglogrepl.DropReplicationSlot(ctx, replConn, slotName, pglogrepl.DropReplicationSlotOptions{})
}

// DropPublication drops a publication.
func (c *Conn) DropPublication(ctx context.Context, pubName string) error {
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{pubName}.Sanitize()))
	return err
}

// PostgresVersion returns the server version string.
func (c *Conn) PostgresVersion(ctx context.Context) (string, error) {
	var version string
	err := c.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	return version, err
}

func (c *Conn) connectReplication(ctx context.Context) (*pgconn.PgConn, error) {
	connStr := c.url
	if strings.Contains(connStr, "?") {
		connStr += "&replication=database"
	} else {
		connStr += "?replication=database"
	}
	return pgconn.Connect(ctx, connStr)
}
