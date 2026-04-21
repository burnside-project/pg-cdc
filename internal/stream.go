package internal

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// Start begins WAL streaming and writes delta Parquet files.
func Start(ctx context.Context, cfg config.Config, source ports.Source, sink ports.Sink) error {
	defer source.Close()

	// Load manifest
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest (run 'pg-cdc init' first): %w", err)
	}

	slotName := cfg.Replication.Slot
	if slotName == "" {
		slotName = "pg_cdc_slot"
	}
	pubName := cfg.Replication.Publication
	if pubName == "" {
		pubName = "pg_cdc_pub"
	}

	flushInterval := time.Duration(cfg.Flush.IntervalSec) * time.Second
	if flushInterval == 0 {
		flushInterval = 10 * time.Second
	}
	flushMaxRows := cfg.Flush.MaxRows
	if flushMaxRows == 0 {
		flushMaxRows = 1000
	}

	fmt.Printf("Streaming from slot=%s pub=%s (flush: %s / %d rows)\n", slotName, pubName, flushInterval, flushMaxRows)
	fmt.Printf("Starting from LSN: %s\n", m.Source.InitLSN)

	// Start streaming
	events := make(chan ports.CDCEvent, 1000)
	var streamErr error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		streamErr = source.Stream(ctx, slotName, pubName, m.Source.InitLSN, events)
	}()

	// Buffer and flush
	buffer := &eventBuffer{
		source:   source,
		sink:     sink,
		manifest: m,
		tables:   m.ActiveTables(),
	}

	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case evt, ok := <-events:
			if !ok {
				// Stream closed — flush remaining
				if err := buffer.flush(ctx); err != nil {
					fmt.Printf("flush error: %v\n", err)
				}
				wg.Wait()
				return streamErr
			}
			buffer.add(evt)
			if buffer.count() >= flushMaxRows {
				if err := buffer.flush(ctx); err != nil {
					fmt.Printf("flush error: %v\n", err)
				}
			}

		case <-flushTicker.C:
			if buffer.count() > 0 {
				if err := buffer.flush(ctx); err != nil {
					fmt.Printf("flush error: %v\n", err)
				}
			}

		case <-ctx.Done():
			if err := buffer.flush(ctx); err != nil {
				fmt.Printf("flush error: %v\n", err)
			}
			wg.Wait()
			return ctx.Err()
		}
	}
}

// eventBuffer collects CDC events and flushes them as epoch Parquet files.
type eventBuffer struct {
	source   ports.Source
	sink     ports.Sink
	manifest *manifest.Manifest
	tables   map[string]manifest.Table
	events   []ports.CDCEvent
	mu       sync.Mutex
}

// evolveSchema checks events for columns not present in the manifest schema
// and refreshes the schema from the source if any are found. New columns get
// appended to preserve existing column ordering in downstream Parquet files.
// Returns true if the schema was updated.
func (b *eventBuffer) evolveSchema(ctx context.Context, tableName string, events []ports.CDCEvent) (bool, error) {
	schema, ok := b.manifest.Schemas[tableName]
	if !ok {
		return false, nil
	}

	known := make(map[string]struct{}, len(schema.Columns))
	for _, c := range schema.Columns {
		known[c.Name] = struct{}{}
	}

	// Collect unknown column names, skipping CDC metadata.
	unknown := make(map[string]struct{})
	for _, evt := range events {
		for k := range evt.Row {
			if k == btypes.ColOp || k == btypes.ColEpoch || k == btypes.ColDeletedAt {
				continue
			}
			if _, ok := known[k]; !ok {
				unknown[k] = struct{}{}
			}
		}
	}
	if len(unknown) == 0 {
		return false, nil
	}

	// Refresh schema from source.
	parts := strings.SplitN(tableName, ".", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid table name %q", tableName)
	}
	cols, err := b.source.GetTableSchema(ctx, parts[0], parts[1])
	if err != nil {
		return false, fmt.Errorf("refresh schema for %s: %w", tableName, err)
	}

	// Append new columns in source order, preserving existing column order.
	added := 0
	for _, c := range cols {
		if _, ok := known[c.Name]; ok {
			continue
		}
		if _, ok := unknown[c.Name]; !ok {
			continue
		}
		schema.Columns = append(schema.Columns, manifest.Column{
			Name:        c.Name,
			PgType:      c.Type,
			ParquetType: btypes.ParquetType(c.Type),
			Nullable:    c.Nullable,
		})
		added++
	}
	if added == 0 {
		return false, nil
	}

	schema.Version++
	b.manifest.Schemas[tableName] = schema
	fmt.Printf("  schema evolved: %s (+%d cols, v=%d)\n", tableName, added, schema.Version)
	return true, nil
}

func (b *eventBuffer) add(evt ports.CDCEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, evt)
}

func (b *eventBuffer) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}

func (b *eventBuffer) flush(ctx context.Context) error {
	b.mu.Lock()
	if len(b.events) == 0 {
		b.mu.Unlock()
		return nil
	}
	events := b.events
	b.events = nil
	b.mu.Unlock()

	// Group events by table
	byTable := make(map[string][]ports.CDCEvent)
	for _, evt := range events {
		byTable[evt.Table] = append(byTable[evt.Table], evt)
	}

	// Determine next epoch number
	var maxEpoch int64
	for _, t := range b.manifest.Tables {
		if t.LatestDeltaEpoch > maxEpoch {
			maxEpoch = t.LatestDeltaEpoch
		}
	}
	nextEpoch := maxEpoch + 1

	// Write delta Parquet for each table
	for tableName, tableEvents := range byTable {
		table, ok := b.tables[tableName]
		if !ok {
			fmt.Printf("  skip: %s (not in manifest)\n", tableName)
			continue
		}

		// Detect and absorb schema additions (new columns via ALTER TABLE).
		if _, err := b.evolveSchema(ctx, tableName, tableEvents); err != nil {
			fmt.Printf("  schema refresh failed for %s: %v\n", tableName, err)
		}

		// Build rows with __op
		rows := make([]writer.Row, len(tableEvents))
		for i, evt := range tableEvents {
			row := make(writer.Row, len(evt.Row)+1)
			row[btypes.ColOp] = evt.Op
			for k, v := range evt.Row {
				row[k] = v
			}
			rows[i] = row
		}

		// Get column definitions from manifest schema
		schema, ok := b.manifest.Schemas[tableName]
		if !ok {
			fmt.Printf("  skip: %s (no schema in manifest)\n", tableName)
			continue
		}

		// Write to temp, then upload
		deltaFilename := epoch.FormatFilename(nextEpoch)
		remotePath := filepath.Join(table.Path, "deltas", deltaFilename)
		tmpPath := filepath.Join("/tmp", "pgcdc-stream", remotePath)

		if err := writer.WriteDeltaParquet(tmpPath, rows, schema.Columns, nextEpoch); err != nil {
			return fmt.Errorf("write delta for %s: %w", tableName, err)
		}

		if err := uploadFile(ctx, b.sink, remotePath, tmpPath); err != nil {
			return fmt.Errorf("upload delta for %s: %w", tableName, err)
		}

		// Update manifest
		t := b.manifest.Tables[tableName]
		t.LatestDeltaEpoch = nextEpoch
		b.manifest.Tables[tableName] = t
		b.tables[tableName] = t

		fmt.Printf("  epoch %d: %s (%d events)\n", nextEpoch, tableName, len(tableEvents))
	}

	// Update manifest
	b.manifest.UpdatedAt = time.Now().UTC()
	if err := b.sink.WriteManifest(ctx, b.manifest); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	return nil
}
