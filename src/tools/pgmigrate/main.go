// Command pgmigrate copies a whatsmeow SQLite session store into a Postgres
// database. It lets whatsmeow create the (correct) Postgres schema, then copies
// every whatsmeow_* table row-by-row, converting values to the destination
// column types. FK triggers are disabled for the load so table order is
// irrelevant. Existing rows are kept (ON CONFLICT DO NOTHING), so the tool is
// safe to re-run for an incremental top-up during the cutover window.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func main() {
	src := flag.String("src", "", "source sqlite file, e.g. file:copy.db")
	dst := flag.String("dst", "", "destination postgres uri")
	flag.Parse()
	if *src == "" || *dst == "" {
		log.Fatal("usage: pgmigrate -src <sqlite> -dst <postgres uri>")
	}
	ctx := context.Background()

	// 1) Let whatsmeow create/upgrade the schema in Postgres (New auto-upgrades).
	if _, err := sqlstore.New(ctx, "postgres", *dst, waLog.Noop); err != nil {
		log.Fatalf("create postgres schema: %v", err)
	}
	log.Println("postgres whatsmeow schema ready")

	sq, err := sql.Open("sqlite3", strings.TrimPrefix(*src, "file:"))
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer sq.Close()
	pg, err := sql.Open("postgres", *dst)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	defer pg.Close()

	conn, err := pg.Conn(ctx)
	if err != nil {
		log.Fatalf("pg conn: %v", err)
	}
	defer conn.Close()
	// Disable FK/triggers for the bulk load so copy order does not matter.
	if _, err := conn.ExecContext(ctx, "SET session_replication_role = replica"); err != nil {
		log.Fatalf("disable triggers: %v", err)
	}

	tables := whatsmeowTables(ctx, sq)
	// whatsmeow_version is owned by the schema upgrade; never overwrite it.
	tables = without(tables, "whatsmeow_version")

	// Empty every destination table first so a re-run (or the cutover top-up)
	// is a clean full reload. CASCADE + disabled triggers make order irrelevant.
	if _, err := conn.ExecContext(ctx, "TRUNCATE "+strings.Join(tables, ",")+" CASCADE"); err != nil {
		log.Fatalf("truncate: %v", err)
	}

	log.Printf("copying %d tables", len(tables))
	var grandTotal int64
	for _, t := range tables {
		start := time.Now()
		n, err := copyTable(ctx, sq, conn, t)
		if err != nil {
			log.Fatalf("copy %s: %v", t, err)
		}
		grandTotal += n
		log.Printf("  %-38s %8d rows  %s", t, n, time.Since(start).Round(time.Millisecond))
	}
	log.Printf("done: %d rows across %d tables", grandTotal, len(tables))
}

func without(list []string, drop string) []string {
	out := list[:0]
	for _, v := range list {
		if v != drop {
			out = append(out, v)
		}
	}
	return out
}

// whatsmeowTables lists whatsmeow_* tables present in the source, with the
// device table first so its rows exist before dependents (belt-and-suspenders
// on top of the disabled triggers).
func whatsmeowTables(ctx context.Context, sq *sql.DB) []string {
	rows, err := sq.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'whatsmeow_%' ORDER BY name`)
	if err != nil {
		log.Fatalf("list tables: %v", err)
	}
	defer rows.Close()
	var rest []string
	var device bool
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			log.Fatal(err)
		}
		if n == "whatsmeow_device" {
			device = true
		} else {
			rest = append(rest, n)
		}
	}
	out := make([]string, 0, len(rest)+1)
	if device {
		out = append(out, "whatsmeow_device")
	}
	return append(out, rest...)
}

// copyTable bulk-loads one table using the Postgres COPY protocol (pq.CopyIn),
// which is orders of magnitude faster than per-row INSERTs for large tables.
func copyTable(ctx context.Context, sq *sql.DB, pg *sql.Conn, table string) (int64, error) {
	pgTypes, err := columnTypes(ctx, pg, table)
	if err != nil {
		return 0, fmt.Errorf("pg column types: %w", err)
	}
	if len(pgTypes) == 0 {
		return 0, fmt.Errorf("table %s missing in postgres", table)
	}

	rows, err := sq.QueryContext(ctx, "SELECT * FROM "+table)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	txn, err := pg.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	stmt, err := txn.PrepareContext(ctx, pq.CopyIn(table, cols...))
	if err != nil {
		_ = txn.Rollback()
		return 0, err
	}

	var total int64
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			_ = txn.Rollback()
			return total, err
		}
		vals := make([]any, len(cols))
		for i, c := range cols {
			vals[i] = convert(raw[i], pgTypes[strings.ToLower(c)])
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			_ = txn.Rollback()
			return total, fmt.Errorf("buffer row: %w", err)
		}
		total++
	}
	if err := rows.Err(); err != nil {
		_ = txn.Rollback()
		return total, err
	}
	if _, err := stmt.ExecContext(ctx); err != nil { // flush COPY
		_ = txn.Rollback()
		return total, fmt.Errorf("flush copy: %w", err)
	}
	if err := stmt.Close(); err != nil {
		_ = txn.Rollback()
		return total, err
	}
	if err := txn.Commit(); err != nil {
		return total, err
	}
	return total, nil
}

func columnTypes(ctx context.Context, pg *sql.Conn, table string) (map[string]string, error) {
	rows, err := pg.QueryContext(ctx,
		`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		m[strings.ToLower(name)] = typ
	}
	return m, rows.Err()
}

// convert coerces a value read from SQLite into what the Postgres column type
// expects. The mattn sqlite driver yields int64, float64, string, []byte, or
// nil.
func convert(v any, pgType string) any {
	if v == nil {
		return nil
	}
	switch pgType {
	case "boolean":
		switch t := v.(type) {
		case int64:
			return t != 0
		case bool:
			return t
		case []byte:
			return string(t) == "1" || strings.EqualFold(string(t), "true")
		}
	case "bytea":
		switch t := v.(type) {
		case []byte:
			return t
		case string:
			return []byte(t)
		}
	case "text", "character varying", "uuid", "character":
		switch t := v.(type) {
		case []byte:
			return string(t)
		}
	case "timestamp with time zone", "timestamp without time zone":
		switch t := v.(type) {
		case int64:
			return time.Unix(t, 0)
		case []byte:
			return string(t)
		}
	}
	return v
}
