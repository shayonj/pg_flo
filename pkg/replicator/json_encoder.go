package replicator

import (
	"context"
	"fmt"

	"github.com/pgflo/pg_flo/pkg/utils"
)

// InitializeOIDMap initializes the OID to type name map with custom types from the database
func InitializeOIDMap(ctx context.Context, conn StandardConnection) error {
	rows, err := conn.Query(ctx, `
		SELECT oid, typname
		FROM pg_type
		WHERE typtype = 'b' AND oid > 10000 -- Only base types and custom types
	`)
	if err != nil {
		return fmt.Errorf("failed to query pg_type: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid uint32
		var typeName string
		if err := rows.Scan(&oid, &typeName); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		utils.OidToTypeName[oid] = typeName
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over rows: %w", err)
	}

	return nil
}
