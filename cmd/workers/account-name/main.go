package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"regexp"

	"github.com/glendc/data-ingestion-challenge/pkg"
	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"

	_ "github.com/lib/pq"
)

// cmd redis-specific flags
var (
	pgAddress  string
	pgUser     string
	pgPassword string
	pgDatabase string
	pgTable    string
	pgSSLMode  string
)

// Postgres SSL mode can be one of following
// More info: https://godoc.org/github.com/lib/pq
var sslModeEnum = []string{"disable", "require", "verify-ca", "verify-full"}

// validate tablename via a regexp to prevent sql-injection attacks,
// not required for arguments as they can use the available variable placeholders
// simplified version of SQL Syntax Identifier as described in official docs:
// https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
const reTableName = `^[a-z_][a-zA-Z0-9_]*$`

var rexTableName = regexp.MustCompile(reTableName)

// properties used in postgres records
const (
	propUsername  = "username"
	propTimestamp = "timestamp"
)

// create a new postgres runtime client
func newRuntime() (*runtime, error) {
	uri := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
		pgUser, pgPassword, pgAddress, pgDatabase, pgSSLMode)
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("postgres is not reachable: %q", err)
	}

	resp, err := db.Query(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (%s text unique, %s integer);`,
		pgTable, propUsername, propTimestamp))
	if err != nil {
		return nil, err
	}
	resp.Close()

	return &runtime{
		db: db,
	}, nil
}

type runtime struct {
	db *sql.DB
}

func (rt *runtime) Consume(event *pkg.Event) *rpc.ConsumeError {
	inserted, err := rt.record(event)
	if err != nil {
		// requeue is required as this is a mistake on our part
		// perhaps another accountName worker can handle this
		// or we can try again later
		return rpc.NewConsumeError(err, true)
	}

	if inserted {
		log.Infof("recorded first time event for %q", *event.Username)
	}

	return nil
}

func (rt *runtime) Close() error {
	return rt.db.Close()
}

// record ftue into Postgres
// there is only one record inserted per user,
// which is the first event that gets recorded for that user.
// After that nothing will be inserted and the returned boolean will be false.
func (rt *runtime) record(event *pkg.Event) (bool, error) {
	resp, err := rt.db.Query(fmt.Sprintf(
		`INSERT INTO %s VALUES($1, $2) ON CONFLICT (%s) DO NOTHING RETURNING *;`,
		pgTable, propUsername), *event.Username, *event.Timestamp)
	if err != nil {
		return false, err
	}
	defer resp.Close()
	return resp.Next(), nil
}

func validateSSLMode() bool {
	for sslModeIndex := range sslModeEnum {
		if sslModeEnum[sslModeIndex] == pgSSLMode {
			return true
		}
	}

	return false
}

// ensure given flags make sense
func validateFlags() error {
	if pgAddress == "" {
		return errors.New("postgres instance's address not given, while this is required")
	}
	if pgUser == "" {
		return errors.New("postgres username not given, while this is required")
	}
	if pgDatabase == "" {
		return errors.New("postgres database's name not given, while this is required")
	}
	if !rexTableName.MatchString(pgTable) {
		return fmt.Errorf(
			"postgres table's name %q is not valid according to regexp(%q)",
			pgTable, reTableName)
	}
	if !validateSSLMode() {
		return fmt.Errorf("postgres ssl-mode has to be one of %v", sslModeEnum)
	}

	return nil
}

func main() {
	flag.Parse() // parse all (non-)specific flags
	err := validateFlags()
	if err != nil {
		flag.Usage()
		log.Errorf("invalid flag: %q", err)
	}

	rt, err := newRuntime()
	if err != nil {
		log.Errorf("couldn't create postgres runtime: %q", err)
	}
	defer rt.Close()

	cfg := rpc.NewAMQPConsConfig().WithName("accountName")
	consumer, err := rpc.NewAMQPConsumer(cfg)
	if err != nil {
		log.Errorf("couldn't create consumer: %q", err)
	}
	defer consumer.Close()

	// Listen & Consume Loop
	consumer.ListenAndConsume(rt.Consume)
}

func init() {
	flag.StringVar(&pgAddress, "address", "localhost:5432", "postgres address")
	flag.StringVar(&pgUser, "user", "postgres", "postgres user")
	flag.StringVar(&pgPassword, "password", "", "postgres password")
	flag.StringVar(&pgDatabase, "db", "postgres", "postgres database")
	flag.StringVar(&pgTable, "table", "accountNames",
		"postgres table to use to store accountNames events")
	flag.StringVar(&pgSSLMode, "ssl-mode", "disable",
		fmt.Sprintf("ssl mode used to connect to postgreg %v", sslModeEnum))
}
