// Package tags resolves table tags and policies for CDC filtering.
// Tags classify tables (pii, high_volume, ephemeral). Policies map
// tags to include/exclude decisions. Glob patterns support convention-based tagging.
package tags

import (
	"path/filepath"
	"strings"

	"github.com/burnside-project/pg-cdc-core/internal/config"
)

// Resolution is the result of applying tags + policies to a table.
type Resolution struct {
	Table    string
	Tags     []string
	Included bool
	Reason   string // why excluded (e.g., "policy:pii", "explicit exclude")
}

// Resolve applies tags and policies to a list of discovered tables.
// Returns a Resolution for each table indicating whether it should be replicated.
func Resolve(tables []string, cfg config.TablesConfig) []Resolution {
	// Build explicit exclude set
	excludeSet := make(map[string]bool, len(cfg.Exclude))
	for _, pattern := range cfg.Exclude {
		excludeSet[pattern] = true
	}

	var results []Resolution
	for _, table := range tables {
		res := resolveTable(table, cfg, excludeSet)
		results = append(results, res)
	}
	return results
}

func resolveTable(table string, cfg config.TablesConfig, excludeSet map[string]bool) Resolution {
	// Check explicit exclude (exact match or glob)
	if isExcluded(table, excludeSet, cfg.Exclude) {
		return Resolution{
			Table:    table,
			Included: false,
			Reason:   "explicit exclude",
		}
	}

	// Resolve tags
	tags := resolveTags(table, cfg.Tags)

	// Apply policies
	for _, tag := range tags {
		policy, ok := cfg.Policy[tag]
		if ok && policy == "exclude" {
			return Resolution{
				Table:    table,
				Tags:     tags,
				Included: false,
				Reason:   "policy:" + tag,
			}
		}
	}

	// Check untagged policy
	if len(tags) == 0 {
		policy, ok := cfg.Policy["untagged"]
		if ok && policy == "exclude" {
			return Resolution{
				Table:    table,
				Included: false,
				Reason:   "policy:untagged",
			}
		}
	}

	return Resolution{
		Table:    table,
		Tags:     tags,
		Included: true,
	}
}

// resolveTags returns all tags that match a table via exact match or glob.
func resolveTags(table string, tagDefs map[string][]string) []string {
	var matched []string
	for tag, patterns := range tagDefs {
		for _, pattern := range patterns {
			if matchGlob(table, pattern) {
				matched = append(matched, tag)
				break
			}
		}
	}
	return matched
}

// isExcluded checks if a table matches any exclude pattern.
func isExcluded(table string, exactSet map[string]bool, patterns []string) bool {
	if exactSet[table] {
		return true
	}
	for _, pattern := range patterns {
		if strings.Contains(pattern, "*") || strings.Contains(pattern, "?") {
			if matchGlob(table, pattern) {
				return true
			}
		}
	}
	return false
}

// matchGlob matches a qualified table name against a glob pattern.
// Supports: "billing.*", "*.tbl_session*", "public.tbl_cache_*"
func matchGlob(table, pattern string) bool {
	matched, _ := filepath.Match(pattern, table)
	return matched
}
