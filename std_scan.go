package sqlee

import (
	"context"
	"database/sql"

	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
)

func (s *Std) selectScanExists(ctx context.Context, stmt *sql.Stmt, args []interface{}, dest interface{}) (exists bool, err error) {
	if dest == nil {
		return false, ErrNoDest
	}

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	err = sqlx.StructScan(rows, dest)
	if err != nil {
		return false, err
	}

	return true, nil
}
func (s *Std) selectRangeScan(ctx context.Context, stmt *sql.Stmt, args []interface{}, dest interface{}, handleRow func()) error {
	if dest == nil {
		return ErrNoDest
	}

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return err
	}

	rowsx := sqlx.Rows{
		Rows:   rows,
		Mapper: s.scan.Mapper,
	}

	for rowsx.Next() {
		err = rowsx.StructScan(dest)
		if err != nil {
			return err
		}

		handleRow()
	}
	if rowsx.Err() != nil {
		return err
	}

	return nil
}

// --

func (s *Std) SelectScan(ctx context.Context, query string, args []interface{}, dest interface{}) error {
	_, err := s.SelectScanExists(ctx, query, args, dest)
	return err
}
func (s *Std) SelectScanSb(ctx context.Context, sb SelectBuilder, dest interface{}) error {
	_, err := s.SelectScanExistsSb(ctx, sb, dest)
	return err
}
func (s *Std) SelectScanExists(ctx context.Context, query string, args []interface{}, dest interface{}) (exists bool, err error) {
	stmt, err := s.Cache.PrepareContext(ctx, query)
	if err != nil {
		return false, err
	}

	return s.selectScanExists(ctx, stmt, args, dest)
}
func (s *Std) SelectScanExistsSb(ctx context.Context, sb SelectBuilder, dest interface{}) (exists bool, err error) {
	query, args, err := sb.ToSql()
	if err != nil {
		return false, err
	}

	return s.SelectScanExists(ctx, query, args, dest)
}
func (s *Std) SelectRangeScan(ctx context.Context, query string, args []interface{}, dest interface{}, handleRow func()) error {
	stmt, err := s.Cache.PrepareContext(ctx, query)
	if err != nil {
		return err
	}

	return s.selectRangeScan(ctx, stmt, args, dest, handleRow)
}
func (s *Std) SelectRangeScanSb(ctx context.Context, sb SelectBuilder, dest interface{}, handleRow func()) error {
	query, args, err := sb.ToSql()
	if err != nil {
		return err
	}

	return s.SelectRangeScan(ctx, query, args, dest, handleRow)
}

// --

func (s *Std) SelectScanTx(ctx context.Context, tx *sql.Tx, query string, args []interface{}, dest interface{}) error {
	_, err := s.SelectScanExistsTx(ctx, tx, query, args, dest)
	return err
}
func (s *Std) SelectScanSbTx(ctx context.Context, tx *sql.Tx, sb SelectBuilder, dest interface{}) error {
	_, err := s.SelectScanExistsSbTx(ctx, tx, sb, dest)
	return err
}
func (s *Std) SelectScanExistsTx(ctx context.Context, tx *sql.Tx, query string, args []interface{}, dest interface{}) (exists bool, err error) {
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return false, err
	}
	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	return s.selectScanExists(ctx, stmt, args, dest)
}
func (s *Std) SelectScanExistsSbTx(ctx context.Context, tx *sql.Tx, sb SelectBuilder, dest interface{}) (exists bool, err error) {
	query, args, err := sb.ToSql()
	if err != nil {
		return false, err
	}

	return s.SelectScanExistsTx(ctx, tx, query, args, dest)
}
func (s *Std) SelectRangeScanTx(ctx context.Context, tx *sql.Tx, query string, args []interface{}, dest interface{}, handleRow func()) (err error) {
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	return s.selectRangeScan(ctx, stmt, args, dest, handleRow)
}
func (s *Std) SelectRangeScanSbTx(ctx context.Context, tx *sql.Tx, sb SelectBuilder, dest interface{}, handleRow func()) error {
	query, args, err := sb.ToSql()
	if err != nil {
		return err
	}

	return s.SelectRangeScanTx(ctx, tx, query, args, dest, handleRow)
}
