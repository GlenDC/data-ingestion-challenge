package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"

	"gopkg.in/pg.v5"
)

// cmd redis-specific flags
var (
	pgAddress  = flag.String("address", "localhost:5432", "postgres address")
	pgUser     = flag.String("user", "postgres", "postgres user")
	pgPassword = flag.String("password", "", "postgres password")
	pgDatabase = flag.String("db", "postgres", "postgres database")
	pgTable    = flag.String("table", "ftues", "postgres table to use to store ftue events")
)

// event struct as this worker expects it
type ftue struct {
	Username *string
	Date     *time.Time
}

func (e *ftue) String() string {
	return fmt.Sprintf("%s<%s %s>", *pgTable, *e.Username, e.Date.String())
}

// Validate the ftue event
func (e *ftue) Validate() error {
	if e.Username == nil {
		return errors.New("required username property not given")
	}
	if e.Date == nil {
		return errors.New("required date property not given")
	}

	return nil
}

// create a new postgres runtime client
func newRuntime() (*runtime, error) {
	db := pg.Connect(&pg.Options{
		User:     *pgUser,
		Password: *pgPassword,
		Database: *pgDatabase,
		Addr:     *pgAddress,
	})

	q := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (username text unique, date timestamptz)`,
		*pgTable)
	if _, err := db.Exec(q); err != nil {
		return nil, err
	}

	return &runtime{
		db: db,
	}, nil
}

type runtime struct {
	db *pg.DB
}

func (rt *runtime) Consume(raw []byte) *rpc.ConsumeError {
	var event ftue
	// try to unpack raw data as a daily event
	if err := rpc.UnmarshalConsumption(raw, &event); err != nil {
		return err
	}

	// validate data
	if err := event.Validate(); err != nil {
		// requeue is not required as this data is not valid
		return rpc.NewConsumeError(err, false)
	}

	inserted, err := rt.record(&event)
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
func (rt *runtime) record(event *ftue) (bool, error) {
	resp, err := rt.db.Model(event).OnConflict("(username) DO NOTHING").Insert()
	if err != nil {
		return false, err
	}

	inserted := resp.RowsAffected() > 0
	return inserted, nil
}

func main() {
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
