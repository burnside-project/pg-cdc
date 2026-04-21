package postgres

import (
	"context"
	"fmt"

	"github.com/burnside-project/burnside-go/manifest"
)

// TableGrant represents a SELECT grant on a table for a role.
type TableGrant struct {
	Role   string
	Schema string
	Table  string
}

// ColumnGrant represents a SELECT grant on specific columns for a role.
type ColumnGrant struct {
	Role   string
	Schema string
	Table  string
	Column string
}

// RoleMembership represents a role hierarchy entry.
type RoleMembership struct {
	Role     string
	MemberOf string
}

// GetTableGrants returns all SELECT grants from information_schema.table_privileges.
func (c *Conn) GetTableGrants(ctx context.Context) ([]TableGrant, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT grantee, table_schema, table_name
		 FROM information_schema.table_privileges
		 WHERE privilege_type = 'SELECT'
		 AND table_schema NOT IN ('pg_catalog', 'information_schema')
		 ORDER BY grantee, table_schema, table_name`)
	if err != nil {
		return nil, fmt.Errorf("query table grants: %w", err)
	}
	defer rows.Close()

	var grants []TableGrant
	for rows.Next() {
		var g TableGrant
		if err := rows.Scan(&g.Role, &g.Schema, &g.Table); err != nil {
			return nil, err
		}
		grants = append(grants, g)
	}
	return grants, rows.Err()
}

// GetColumnGrants returns all SELECT column grants from information_schema.column_privileges.
func (c *Conn) GetColumnGrants(ctx context.Context) ([]ColumnGrant, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT grantee, table_schema, table_name, column_name
		 FROM information_schema.column_privileges
		 WHERE privilege_type = 'SELECT'
		 AND table_schema NOT IN ('pg_catalog', 'information_schema')
		 ORDER BY grantee, table_schema, table_name, column_name`)
	if err != nil {
		return nil, fmt.Errorf("query column grants: %w", err)
	}
	defer rows.Close()

	var grants []ColumnGrant
	for rows.Next() {
		var g ColumnGrant
		if err := rows.Scan(&g.Role, &g.Schema, &g.Table, &g.Column); err != nil {
			return nil, err
		}
		grants = append(grants, g)
	}
	return grants, rows.Err()
}

// GetRoleMemberships returns the role hierarchy from pg_auth_members.
func (c *Conn) GetRoleMemberships(ctx context.Context) ([]RoleMembership, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT r.rolname AS role, m.rolname AS member
		 FROM pg_auth_members am
		 JOIN pg_roles r ON am.roleid = r.oid
		 JOIN pg_roles m ON am.member = m.oid
		 ORDER BY r.rolname, m.rolname`)
	if err != nil {
		return nil, fmt.Errorf("query role memberships: %w", err)
	}
	defer rows.Close()

	var memberships []RoleMembership
	for rows.Next() {
		var m RoleMembership
		if err := rows.Scan(&m.Role, &m.MemberOf); err != nil {
			return nil, err
		}
		memberships = append(memberships, m)
	}
	return memberships, rows.Err()
}

// BuildRoleProfiles reads PostgreSQL ACLs and builds manifest.Role entries
// for the specified roles. Only roles in the allowList are included.
func (c *Conn) BuildRoleProfiles(ctx context.Context, allowList []string) (map[string]manifest.Role, error) {
	if len(allowList) == 0 {
		return nil, nil
	}

	allowSet := make(map[string]bool, len(allowList))
	for _, r := range allowList {
		allowSet[r] = true
	}

	// Get table-level grants
	tableGrants, err := c.GetTableGrants(ctx)
	if err != nil {
		return nil, err
	}

	// Get column-level grants
	colGrants, err := c.GetColumnGrants(ctx)
	if err != nil {
		return nil, err
	}

	// Build per-role table access: role → table → columns
	type tableAccess struct {
		allColumns bool
		columns    map[string]bool
	}
	roleAccess := make(map[string]map[string]*tableAccess)

	// Table-level grants → all columns
	for _, g := range tableGrants {
		if !allowSet[g.Role] {
			continue
		}
		qualified := g.Schema + "." + g.Table
		if roleAccess[g.Role] == nil {
			roleAccess[g.Role] = make(map[string]*tableAccess)
		}
		roleAccess[g.Role][qualified] = &tableAccess{allColumns: true}
	}

	// Column-level grants → specific columns (only if no table-level grant exists)
	for _, g := range colGrants {
		if !allowSet[g.Role] {
			continue
		}
		qualified := g.Schema + "." + g.Table
		if roleAccess[g.Role] == nil {
			roleAccess[g.Role] = make(map[string]*tableAccess)
		}
		existing := roleAccess[g.Role][qualified]
		if existing != nil && existing.allColumns {
			continue // table-level grant already covers all columns
		}
		if existing == nil {
			existing = &tableAccess{columns: make(map[string]bool)}
			roleAccess[g.Role][qualified] = existing
		}
		existing.columns[g.Column] = true
	}

	// Convert to manifest.Role
	roles := make(map[string]manifest.Role, len(roleAccess))
	for roleName, tables := range roleAccess {
		role := manifest.Role{
			Tables: make(map[string]manifest.RoleAccess, len(tables)),
		}
		for tableName, access := range tables {
			if access.allColumns {
				role.Tables[tableName] = manifest.RoleAccess{AllColumns: true}
			} else {
				cols := make([]string, 0, len(access.columns))
				for col := range access.columns {
					cols = append(cols, col)
				}
				role.Tables[tableName] = manifest.RoleAccess{Columns: cols}
			}
		}
		roles[roleName] = role
	}

	return roles, nil
}
