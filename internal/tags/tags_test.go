package tags

import (
	"testing"

	"github.com/burnside-project/pg-cdc-core/internal/config"
)

func TestResolve_BasicTagsAndPolicies(t *testing.T) {
	cfg := config.TablesConfig{
		Exclude: []string{"public.tbl_sessions"},
		Tags: map[string][]string{
			"pii":       {"public.tbl_cc", "public.tbl_user_emails", "billing.*"},
			"ephemeral": {"*.tbl_session*", "*.tbl_cache_*"},
		},
		Policy: map[string]string{
			"pii":       "exclude",
			"ephemeral": "exclude",
			"untagged":  "include",
		},
	}

	tables := []string{
		"public.orders",
		"public.products",
		"public.tbl_cc",
		"public.tbl_user_emails",
		"public.tbl_sessions",
		"public.tbl_cache_items",
		"billing.payments",
		"billing.invoices",
	}

	results := Resolve(tables, cfg)

	expected := map[string]bool{
		"public.orders":          true,  // untagged → include
		"public.products":        true,  // untagged → include
		"public.tbl_cc":          false, // pii → exclude
		"public.tbl_user_emails": false, // pii → exclude
		"public.tbl_sessions":    false, // explicit exclude
		"public.tbl_cache_items": false, // ephemeral glob → exclude
		"billing.payments":       false, // billing.* → pii → exclude
		"billing.invoices":       false, // billing.* → pii → exclude
	}

	for _, res := range results {
		want, ok := expected[res.Table]
		if !ok {
			t.Errorf("unexpected table: %s", res.Table)
			continue
		}
		if res.Included != want {
			t.Errorf("%s: included=%v, want %v (reason: %s)", res.Table, res.Included, want, res.Reason)
		}
	}
}

func TestResolve_UntaggedExclude(t *testing.T) {
	cfg := config.TablesConfig{
		Tags: map[string][]string{
			"approved": {"public.orders"},
		},
		Policy: map[string]string{
			"approved": "include",
			"untagged": "exclude",
		},
	}

	results := Resolve([]string{"public.orders", "public.unknown"}, cfg)

	for _, res := range results {
		switch res.Table {
		case "public.orders":
			if !res.Included {
				t.Error("public.orders should be included (tagged approved)")
			}
		case "public.unknown":
			if res.Included {
				t.Error("public.unknown should be excluded (untagged)")
			}
			if res.Reason != "policy:untagged" {
				t.Errorf("reason = %q, want policy:untagged", res.Reason)
			}
		}
	}
}

func TestResolve_GlobPatterns(t *testing.T) {
	cfg := config.TablesConfig{
		Tags: map[string][]string{
			"internal": {"*.tbl_migration*", "*.schema_version"},
		},
		Policy: map[string]string{
			"internal": "exclude",
			"untagged": "include",
		},
	}

	results := Resolve([]string{
		"public.tbl_migrations",
		"public.schema_version",
		"public.orders",
	}, cfg)

	for _, res := range results {
		switch res.Table {
		case "public.tbl_migrations":
			if res.Included {
				t.Error("tbl_migrations should be excluded (internal glob)")
			}
		case "public.schema_version":
			if res.Included {
				t.Error("schema_version should be excluded (internal glob)")
			}
		case "public.orders":
			if !res.Included {
				t.Error("orders should be included")
			}
		}
	}
}

func TestResolve_NoPolicy(t *testing.T) {
	cfg := config.TablesConfig{
		Tags: map[string][]string{
			"high_volume": {"public.events"},
		},
		// No policy defined → tagged tables are included by default
	}

	results := Resolve([]string{"public.events", "public.orders"}, cfg)

	for _, res := range results {
		if !res.Included {
			t.Errorf("%s should be included when no exclusion policy", res.Table)
		}
	}
}

func TestResolve_ExcludeGlob(t *testing.T) {
	cfg := config.TablesConfig{
		Exclude: []string{"*.tbl_cache_*"},
	}

	results := Resolve([]string{"public.tbl_cache_items", "public.orders"}, cfg)

	for _, res := range results {
		switch res.Table {
		case "public.tbl_cache_items":
			if res.Included {
				t.Error("tbl_cache_items should be excluded by glob")
			}
		case "public.orders":
			if !res.Included {
				t.Error("orders should be included")
			}
		}
	}
}
