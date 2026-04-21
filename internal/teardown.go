package internal

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// Teardown drops the publication and replication slot.
func Teardown(ctx context.Context, cfg config.Config, source ports.Source) error {
	defer source.Close()

	pubName := cfg.Replication.Publication
	if pubName == "" {
		pubName = "pg_cdc_pub"
	}
	slotName := cfg.Replication.Slot
	if slotName == "" {
		slotName = "pg_cdc_slot"
	}

	fmt.Printf("Dropping publication: %s\n", pubName)
	if err := source.DropPublication(ctx, pubName); err != nil {
		fmt.Printf("  warning: %v\n", err)
	}

	fmt.Printf("Dropping replication slot: %s\n", slotName)
	if err := source.DropReplicationSlot(ctx, slotName); err != nil {
		fmt.Printf("  warning: %v\n", err)
	}

	fmt.Println("Teardown complete")
	return nil
}
