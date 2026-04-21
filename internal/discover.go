package internal

import (
	"context"
	"fmt"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/tags"
)

// Discover lists tables from the source, applies tags + policies, and optionally shows ACLs.
func Discover(ctx context.Context, cfg config.Config, source ports.Source, showACL bool, dryRun bool) error {
	defer source.Close()

	schemas := cfg.Source.Postgres.Schemas
	if len(schemas) == 0 {
		schemas = []string{"public"}
	}

	var allTables []string
	for _, schema := range schemas {
		tableNames, err := source.ListTables(ctx, schema)
		if err != nil {
			return fmt.Errorf("list tables in %s: %w", schema, err)
		}
		for _, name := range tableNames {
			allTables = append(allTables, schema+"."+name)
		}
	}

	resolutions := tags.Resolve(allTables, cfg.Tables)

	if dryRun {
		fmt.Println("Dry run — no changes applied")
	}

	fmt.Printf("%-40s %-10s %-20s %s\n", "TABLE", "STATUS", "TAGS", "REASON")
	fmt.Printf("%-40s %-10s %-20s %s\n", "-----", "------", "----", "------")

	var included, excluded int
	for _, res := range resolutions {
		status := "include"
		if !res.Included {
			status = "exclude"
			excluded++
		} else {
			included++
		}
		tagStr := ""
		if len(res.Tags) > 0 {
			tagStr = fmt.Sprintf("%v", res.Tags)
		}
		fmt.Printf("%-40s %-10s %-20s %s\n", res.Table, status, tagStr, res.Reason)
	}

	fmt.Printf("\n%d included, %d excluded, %d total\n", included, excluded, included+excluded)

	if showACL && len(cfg.Profiles.Roles) > 0 {
		fmt.Println("\n--- Role Profiles (from Source ACLs) ---")

		roles, err := source.BuildRoleProfiles(ctx, cfg.Profiles.Roles)
		if err != nil {
			return fmt.Errorf("build role profiles: %w", err)
		}

		for roleName, role := range roles {
			displayName := roleName
			if alias, ok := cfg.Profiles.Aliases[roleName]; ok {
				displayName = fmt.Sprintf("%s (alias: %s)", roleName, alias)
			}
			fmt.Printf("Role: %s\n", displayName)
			for tableName, access := range role.Tables {
				if access.AllColumns {
					fmt.Printf("  %-40s (all columns)\n", tableName)
				} else {
					fmt.Printf("  %-40s (%v)\n", tableName, access.Columns)
				}
			}
			fmt.Println()
		}
	}

	return nil
}
