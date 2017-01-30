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

// Record ftue into Postgres
func (rt *runtime) Record(event *ftue) error {
	_, err := rt.db.Model(event).OnConflict("(username) DO NOTHING").Insert()
	return err
}

func main() {
	rt, err := newRuntime()
	if err != nil {
		log.Errorf("couldn't create postgres runtime: %q", err)
	}

	cfg := rpc.NewConsumerConfig().WithName("ftue")
	rpc.Consumer(cfg, func(ch rpc.DeliveryChannel) {
		forever := make(chan bool)

		go func() {
			var event ftue
			var err error

			for d := range ch {
				// process delivery
				if err = rpc.Consume(d, &event); err != nil {
					log.Warningf("event was rejected: %q", err)
					continue
				}

				if err = event.Validate(); err != nil {
					log.Warningf("event was rejected: %q", err)
					d.Reject(false) // delete delivery as its payload is invalid
					continue
				}

				// track event and acknowledge if all is OK
				if err = rt.Record(&event); err != nil {
					log.Warningf("event was rejected due to an internal server error: %q", err)
					d.Reject(true) // requeue as it's an internal server error
					continue
				}

				// acknowledge a tracked event
				log.Infof("event tracked successfully")
				d.Ack(false)
			}
		}()

		log.Infof("Tracking hourly distinct events. To exit press CTRL+C")
		<-forever
	})
}

func init() {
	flag.Parse()
	log.Init()
}
