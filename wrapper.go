package autoprepare

import (
	"context"
	"database/sql"
)

type DB struct {
	*sql.DB
	c *SQLStmtCache
}

type Tx struct {
	*sql.Tx
	c *SQLStmtCache
}

func (c *SQLStmtCache) Wrapper() *DB {
	return &DB{DB: c.c, c: c}
}

func (w *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return w.QueryContext(context.Background(), query, args)
}

func (w *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return w.c.QueryContext(ctx, query, args)
}

func (w *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return w.QueryRowContext(context.Background(), query, args)
}

func (w *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return w.c.QueryRowContext(ctx, query, args)
}

func (w *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return w.ExecContext(context.Background(), query, args)
}

func (w *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return w.c.ExecContext(ctx, query, args)
}

func (w *DB) Begin() (*Tx, error) {
	tx, err := w.c.c.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, c: w.c}, nil
}

func (w *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := w.c.c.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, c: w.c}, nil
}

func (w *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return w.QueryContext(context.Background(), query, args)
}

func (w *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return w.c.QueryContextTx(ctx, w.Tx, query, args)
}

func (w *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return w.QueryRowContext(context.Background(), query, args)
}

func (w *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return w.c.QueryRowContextTx(ctx, w.Tx, query, args)
}

func (w *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return w.ExecContext(context.Background(), query, args)
}

func (w *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return w.c.ExecContextTx(ctx, w.Tx, query, args)
}
