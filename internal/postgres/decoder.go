package postgres

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

// decoder translates pgoutput v2 WAL messages into CDCEvents.
type decoder struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
}

func newDecoder() *decoder {
	return &decoder{relations: make(map[uint32]*pglogrepl.RelationMessageV2)}
}

func (d *decoder) decode(walData []byte, lsn string, ts time.Time) (*CDCEvent, error) {
	msg, err := pglogrepl.ParseV2(walData, false)
	if err != nil {
		return nil, fmt.Errorf("parse v2: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		d.relations[m.RelationID] = m
		return nil, nil

	case *pglogrepl.InsertMessageV2:
		rel := d.relations[m.RelationID]
		if rel == nil {
			return nil, nil
		}
		row := decodeTuple(m.Tuple, rel)
		return &CDCEvent{
			Table:     fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName),
			Op:        "I",
			Row:       row,
			LSN:       lsn,
			Timestamp: ts,
		}, nil

	case *pglogrepl.UpdateMessageV2:
		rel := d.relations[m.RelationID]
		if rel == nil {
			return nil, nil
		}
		row := decodeTuple(m.NewTuple, rel)
		return &CDCEvent{
			Table:     fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName),
			Op:        "U",
			Row:       row,
			LSN:       lsn,
			Timestamp: ts,
		}, nil

	case *pglogrepl.DeleteMessageV2:
		rel := d.relations[m.RelationID]
		if rel == nil {
			return nil, nil
		}
		row := decodeTuple(m.OldTuple, rel)
		return &CDCEvent{
			Table:     fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName),
			Op:        "D",
			Row:       row,
			LSN:       lsn,
			Timestamp: ts,
		}, nil

	case *pglogrepl.BeginMessage:
		return nil, nil
	case *pglogrepl.CommitMessage:
		return nil, nil
	case *pglogrepl.TypeMessageV2:
		return nil, nil
	case *pglogrepl.OriginMessage:
		return nil, nil
	case *pglogrepl.TruncateMessageV2:
		return nil, nil
	case *pglogrepl.StreamStartMessageV2:
		return nil, nil
	case *pglogrepl.StreamStopMessageV2:
		return nil, nil
	case *pglogrepl.StreamCommitMessageV2:
		return nil, nil
	case *pglogrepl.StreamAbortMessageV2:
		return nil, nil
	default:
		return nil, nil
	}
}

func decodeTuple(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) map[string]any {
	if tuple == nil {
		return nil
	}
	row := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			row[colName] = nil
		case 't': // text
			row[colName] = string(col.Data)
		case 'u': // unchanged toast
			row[colName] = nil
		default:
			row[colName] = string(col.Data)
		}
	}
	return row
}
