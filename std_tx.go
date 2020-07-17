package sqlee

import (
	"context"
	"database/sql"

	"github.com/hashicorp/go-multierror"
)

// Tx manages an complete transaction lifecycle with Begin, Commit, Rollback
// and returns any occurring errors.
func (s *Std) Tx(ctx context.Context, handle func(tx *sql.Tx) error) error {
	return s.TxOpts(ctx, nil, handle)
}

// TxOpts manages an complete transaction including specific options with Begin,
// Commit, Rollback and returns any occurring errors.
func (s *Std) TxOpts(ctx context.Context, opts *sql.TxOptions, handle func(tx *sql.Tx) error) error {
	tx, err := s.DB.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	err = handle(tx)
	if err != nil {
		rlbErr := tx.Rollback()
		if rlbErr != nil {
			err = multierror.Append(err, rlbErr)
		}

		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
