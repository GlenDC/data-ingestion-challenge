package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"

	"github.com/glendc/data-ingestion-challenge/pkg"
	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"

	_ "github.com/lib/pq"
)

// cmd redis-specific flags
var (
	pgAddress  = flag.String("address", "localhost:5432", "postgres address")
	pgUser     = flag.String("user", "postgres", "postgres user")
	pgPassword = flag.String("password", "", "postgres password")
	pgDatabase = flag.String("db", "postgres", "postgres database")
	pgTable    = flag.String("table", "accountNames", "postgres table to use to store accountNames events")
)

// create a new postgres runtime client
func newRuntime() (*runtime, error) {
	uri := fmt.Sprintf("postgres://%s:%s@%s/%s",
		*pgUser, *pgPassword, *pgAddress, *pgDatabase)
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return nil, err
	}

	resp, err := db.Query(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (username text unique, timestamp integer);`,
		*pgTable))
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
		`INSERT INTO %s VALUES('%s', %d) ON CONFLICT (username) DO NOTHING;`,
		*pgTable, *event.Username, *event.Timestamp))
	if err != nil {
		return false, err
	}
	defer resp.Close()

	columns, err := resp.Columns()
	inserted := err == nil && len(columns) > 0
	return inserted, nil
}

// ensure given flags make sense
func validateFlags() error {
	if *pgAddress == "" {
		return errors.New("postgres instance's address not given, while this is required")
	}
	if *pgUser == "" {
		return errors.New("postgres username not given, while this is required")
	}
	if *pgDatabase == "" {
		return errors.New("postgres database's name not given, while this is required")
	}
	if *pgTable == "" {
		return errors.New("postgres table's interval has to be positive and non-zero")
	}
	return nil
}

func main() {
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
	flag.Parse()
	log.Init()
}
