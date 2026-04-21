package internal

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// Status shows replication health: slot status, lag, LSN, epochs, tables.
func Status(ctx context.Context, cfg config.Config, source ports.Source, sink ports.Sink) error {
	defer source.Close()

	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	slotName := cfg.Replication.Slot
	if slotName == "" {
		slotName = "pg_cdc_slot"
	}

	// Query slot status
	var slotActive bool
	var confirmedLSN, restartLSN string
	err = source.QueryRow(ctx,
		`SELECT active, confirmed_flush_lsn::text, restart_lsn::text
		 FROM pg_replication_slots WHERE slot_name = $1`, slotName).
		Scan(&slotActive, &confirmedLSN, &restartLSN)

	fmt.Println("pg-cdc status")
	fmt.Println()

	if err != nil {
		fmt.Printf("  Slot: %s (not found)\n", slotName)
	} else {
		activeStr := "inactive"
		if slotActive {
			activeStr = "streaming"
		}
		fmt.Printf("  Slot:          %s (%s)\n", slotName, activeStr)
		fmt.Printf("  Confirmed LSN: %s\n", confirmedLSN)
		fmt.Printf("  Restart LSN:   %s\n", restartLSN)
	}

	fmt.Printf("  Init LSN:      %s\n", m.Source.InitLSN)
	fmt.Printf("  Source:         %s\n", m.Source.Name)
	fmt.Println()

	var active, excluded int
	var maxDelta int64
	for _, t := range m.Tables {
		if t.Status == "active" {
			active++
			if t.LatestDeltaEpoch > maxDelta {
				maxDelta = t.LatestDeltaEpoch
			}
		} else {
			excluded++
		}
	}

	fmt.Printf("  Tables:         %d active, %d excluded\n", active, excluded)
	fmt.Printf("  Latest epoch:   %d\n", maxDelta)
	fmt.Printf("  Storage:        %s (%s)\n", cfg.Storage.Path, sink.Backend())

	if !m.Compaction.LastCompactedAt.IsZero() {
		fmt.Printf("  Last compacted: %s\n", m.Compaction.LastCompactedAt.Format("2006-01-02 15:04:05"))
	}

	if len(m.Roles) > 0 {
		fmt.Printf("  Role profiles:  %d\n", len(m.Roles))
	}

	return nil
}
