package whatsapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/config"
	pkgError "github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/error"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/sqlite"
	"github.com/lib/pq"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// InitWaDB initializes the WhatsApp database connection
func InitWaDB(ctx context.Context, DBURI string) *sqlstore.Container {
	log = waLog.Stdout("Main", config.WhatsappLogLevel, true)
	dbLog := waLog.Stdout("Database", config.WhatsappLogLevel, true)

	storeContainer, err := initDatabase(ctx, dbLog, DBURI)
	if err != nil {
		log.Errorf("Database initialization error: %v", err)
		panic(pkgError.InternalServerError(fmt.Sprintf("Database initialization error: %v", err)))
	}

	return storeContainer
}

// initDatabase creates and returns a database store container based on the configured URI
func initDatabase(ctx context.Context, dbLog waLog.Logger, DBURI string) (*sqlstore.Container, error) {
	// Strip surrounding quotes that may come from .env file parsing
	DBURI = strings.Trim(DBURI, `"'`)

	if strings.HasPrefix(DBURI, "file:") {
		DBURI = sqlite.FormatChatStorageURI(DBURI, true, true)
		return sqlstore.New(ctx, sqlite.DriverName, DBURI, dbLog)
	} else if strings.HasPrefix(DBURI, "postgres:") {
		return initPostgresStore(ctx, dbLog, DBURI)
	}

	return nil, fmt.Errorf("unknown database type: %s. Currently only sqlite3(file:) and postgres are supported", DBURI)
}

// initPostgresStore opens Postgres with a bounded connection pool before handing
// it to whatsmeow, so a large account fleet cannot exhaust Postgres
// max_connections (whatsmeow's own sqlstore.New leaves the pool unbounded).
func initPostgresStore(ctx context.Context, dbLog waLog.Logger, DBURI string) (*sqlstore.Container, error) {
	// Use a real Postgres array parameter for batch session/app-state lookups
	// (their_id = ANY($2)) instead of the fallback IN ($2,$3,...) with one
	// placeholder per element. A status broadcast prefetches sessions for
	// thousands of recipients at once; the placeholder form builds a giant query
	// that Postgres must reparse/replan every call, which starves the status-send
	// path under load. lib/pq is already the registered "postgres" driver.
	sqlstore.PostgresArrayWrapper = pq.Array

	db, err := sql.Open("postgres", DBURI)
	if err != nil {
		return nil, fmt.Errorf("open postgres store: %w", err)
	}
	if maxConns := config.DBMaxOpenConns; maxConns > 0 {
		db.SetMaxOpenConns(maxConns)
		idle := maxConns / 4
		if idle < 2 {
			idle = 2
		}
		db.SetMaxIdleConns(idle)
		db.SetConnMaxLifetime(5 * time.Minute)
	}
	container := sqlstore.NewWithDB(db, "postgres", dbLog)
	if err := container.Upgrade(ctx); err != nil {
		_ = container.Close()
		return nil, fmt.Errorf("upgrade postgres store: %w", err)
	}
	return container, nil
}
