package sqlee

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Std implements the `Essentials` interface with only using the standard
// library `database/sql.DB` internally. This should provide great cross-
// database support.
//
// It is fully transparent and non-magical (e.g. the arguments, etc. are just
// passed into the standard library).
//
// The primary use case of `Std` is to drastically reduce error checking.
type Std struct {
	DB    *sql.DB
	scan  *sqlx.DB
	Cache *StmtCache
}

// NewStd initializes a new *Std
func NewStd(db *sql.DB, driverName string) *Std {
	return &Std{
		DB:    db,
		scan:  sqlx.NewDb(db, driverName),
		Cache: newStmtCache(db),
	}
}
