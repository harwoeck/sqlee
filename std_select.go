package sqlee

import (
	"context"
	"database/sql"

	"github.com/hashicorp/go-multierror"
)

func (s *Std) selectExists(ctx context.Context, stmt *sql.Stmt, args []interface{}, dest []interface{}) (exists bool, err error) {
	if dest == nil || len(dest) == 0 {
		return false, ErrNoDest
	}

	err = stmt.QueryRowContext(ctx, args...).Scan(dest...)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, err
	}

	return true, nil
}
func (s *Std) selectRange(ctx context.Context, stmt *sql.Stmt, args []interface{}, dest []interface{}, handleRow func()) error {
	if dest == nil || len(dest) == 0 {
		return ErrNoDest
	}

	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return err
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return err
		}

		handleRow()
	}
	if rows.Err() != nil {
		return err
	}

	return nil
}

// --

// Select selects the `query` string from the database. The `args` interface
// slice should contain all primitive value arguments. The `dest` interface
// slice should contain a collection of pointer to primitive types. The results
// of the query will be saved into these pointers.
func (s *Std) Select(ctx context.Context, query string, args []interface{}, dest []interface{}) error {
	_, err := s.SelectExists(ctx, query, args, dest)
	return err
}

// SelectSb is the same as `Select` but uses the passed SelectBuilder to
// receive `query` and `args` parameters.
func (s *Std) SelectSb(ctx context.Context, sb SelectBuilder, dest []interface{}) error {
	_, err := s.SelectExistsSb(ctx, sb, dest)
	return err
}

// SelectExists is the same as `Select`, but additionally returns an boolean
// value, whether or not the query returned a row.
func (s *Std) SelectExists(ctx context.Context, query string, args []interface{}, dest []interface{}) (exists bool, err error) {
	stmt, err := s.Cache.PrepareContext(ctx, query)
	if err != nil {
		return false, err
	}

	return s.selectExists(ctx, stmt, args, dest)
}

// SelectExistsSq is the same as `SelectExists` but uses the passed
// squirrel.SelectBuilder to receive `query` and `args` parameters.
func (s *Std) SelectExistsSb(ctx context.Context, sb SelectBuilder, dest []interface{}) (exists bool, err error) {
	query, args, err := sb.ToSql()
	if err != nil {
		return false, err
	}

	return s.SelectExists(ctx, query, args, dest)
}

// SelectRange selects a range of results from the database, defined by the
// `query` and it's arguments. The `args` interface slice should contain all
// primitive value arguments. The `dest` interface slice should contain a
// collection of pointer to primitive types. The results of the query will be
// saved into these pointers.
//
// As soon as one row has been loaded the `handleRow` callback will be called.
// It is the package caller's responsibility to copy the values from the `dest`
// pointers into another data structure. After returning the `handleRow`
// function the values of `dest` will be overwritten with the next row's
// values.
func (s *Std) SelectRange(ctx context.Context, query string, args []interface{}, dest []interface{}, handleRow func()) error {
	stmt, err := s.Cache.PrepareContext(ctx, query)
	if err != nil {
		return err
	}

	return s.selectRange(ctx, stmt, args, dest, handleRow)
}

// SelectRangeSq is the same as `SelectRange` but uses the passed
// squirrel.SelectBuilder to receive `query` and `args` parameters.
func (s *Std) SelectRangeSb(ctx context.Context, sb SelectBuilder, dest []interface{}, handleRow func()) error {
	query, args, err := sb.ToSql()
	if err != nil {
		return err
	}

	return s.SelectRange(ctx, query, args, dest, handleRow)
}

// --

// SelectTx is the same as `Select` but uses the passed transaction `tx` to
// execute the statement.
func (s *Std) SelectTx(ctx context.Context, tx *sql.Tx, query string, args []interface{}, dest []interface{}) error {
	_, err := s.SelectExistsTx(ctx, tx, query, args, dest)
	return err
}

// SelectSqTx is the same as `SelectTx` but uses the passed
// squirrel.SelectBuilder to receive `query` and `args` parameters.
func (s *Std) SelectSbTx(ctx context.Context, tx *sql.Tx, sb SelectBuilder, dest []interface{}) error {
	_, err := s.SelectExistsSbTx(ctx, tx, sb, dest)
	return err
}

// SelectExistsTx is the same as `SelectExists` but uses the passed transaction
// `tx` to execute the statement.
func (s *Std) SelectExistsTx(ctx context.Context, tx *sql.Tx, query string, args []interface{}, dest []interface{}) (exists bool, err error) {
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

	return s.selectExists(ctx, stmt, args, dest)
}

// SelectExistsSqTx is the same as `SelectExistsTx` but uses the passed
// squirrel.SelectBuilder to receive `query` and `args` parameters.
func (s *Std) SelectExistsSbTx(ctx context.Context, tx *sql.Tx, sb SelectBuilder, dest []interface{}) (exists bool, err error) {
	query, args, err := sb.ToSql()
	if err != nil {
		return false, err
	}

	return s.SelectExistsTx(ctx, tx, query, args, dest)
}

// SelectRangeTx is the same as `SelectRange` but uses the passed transaction
// `tx` to execute the statement.
func (s *Std) SelectRangeTx(ctx context.Context, tx *sql.Tx, query string, args []interface{}, dest []interface{}, handleRow func()) (err error) {
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

	return s.selectRange(ctx, stmt, args, dest, handleRow)
}

// SelectRangeSqTx is the same as `SelectRangeTx` but uses the passed
// squirrel.SelectBuilder to receive `query` and `args` parameters.
func (s *Std) SelectRangeSbTx(ctx context.Context, tx *sql.Tx, sb SelectBuilder, dest []interface{}, handleRow func()) error {
	query, args, err := sb.ToSql()
	if err != nil {
		return err
	}

	return s.SelectRangeTx(ctx, tx, query, args, dest, handleRow)
}
