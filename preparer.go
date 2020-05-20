package sqlee

import (
	"context"
	"database/sql"
)

type preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}
