package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// CDCEvent represents a single change captured from WAL.
type CDCEvent struct {
	Table     string         // qualified table name (schema.table)
	Op        string         // "I", "U", "D"
	Row       map[string]any // column values
	LSN       string
	Timestamp time.Time
}

// Stream starts logical replication and sends decoded events to the channel.
// The channel is closed when Stream returns. Blocks until ctx is cancelled.
func (c *Conn) Stream(ctx context.Context, slotName, publicationName, startLSN string, events chan<- CDCEvent) error {
	defer close(events)

	replConn, err := c.connectReplication(ctx)
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	defer func() { _ = replConn.Close(context.Background()) }()

	lsn, err := pglogrepl.ParseLSN(startLSN)
	if err != nil {
		return fmt.Errorf("parse start LSN: %w", err)
	}

	err = pglogrepl.StartReplication(ctx, replConn, slotName, lsn,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '2'",
				fmt.Sprintf("publication_names '%s'", publicationName),
				"messages 'true'",
			},
		})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	// Immediate standby status to prevent wal_sender_timeout
	_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn})

	standbyDeadline := time.Now().Add(10 * time.Second)
	serverWALEnd := lsn
	decoder := newDecoder()

	for {
		if ctx.Err() != nil {
			_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: serverWALEnd})
			return ctx.Err()
		}

		// Periodic standby status
		if time.Now().After(standbyDeadline) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: serverWALEnd}); err != nil {
				return fmt.Errorf("standby status: %w", err)
			}
			standbyDeadline = time.Now().Add(10 * time.Second)
		}

		recvCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		rawMsg, err := replConn.ReceiveMessage(recvCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("replication error: %s", errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ServerWALEnd > serverWALEnd {
				serverWALEnd = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				standbyDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog: %w", err)
			}

			walEnd := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			if walEnd > serverWALEnd {
				serverWALEnd = walEnd
			}

			evt, err := decoder.decode(xld.WALData, xld.WALStart.String(), xld.ServerTime)
			if err != nil {
				return fmt.Errorf("decode WAL: %w", err)
			}
			if evt != nil {
				select {
				case events <- *evt:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

// ConfirmLSN sends a standby status update confirming the given LSN.
func (c *Conn) ConfirmLSN(ctx context.Context, lsn string) error {
	replConn, err := c.connectReplication(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = replConn.Close(ctx) }()

	parsed, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return err
	}
	return pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: parsed})
}
