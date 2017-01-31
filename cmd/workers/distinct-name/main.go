package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/glendc/data-ingestion-challenge/pkg"
	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"

	"gopkg.in/redis.v5"
)

// cmd redis-specific flags
var (
	redisAddress  string
	redisPassword string
	redisDB       int
)

// dailyIDFromEvent returns the daily event data as a key
// using the format: MM.DD.<METRIC>
func dailyIDFromEvent(e *pkg.Event) string {
	date := time.Unix(*e.Timestamp, 0).UTC()
	return fmt.Sprintf("%02d.%02d.%s",
		date.Month(), date.Day(), e.Metric)
}

// create a new redis runtime client
func newRuntime() (*runtime, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: redisPassword,
		DB:       redisDB,
	})

	if _, err := client.Ping().Result(); err != nil {
		return nil, err
	}

	return &runtime{
		client: client,
	}, nil
}

type runtime struct {
	client *redis.Client
}

// Consume raw incoming data as a daily event and record it
// see runtime::Record for more information
func (rt *runtime) Consume(event *pkg.Event) *rpc.ConsumeError {
	if err := rt.record(event); err != nil {
		// requeue is required as this is a mistake on our part
		// perhaps another distinctName worker can handle this
		// or we can try again later
		return rpc.NewConsumeError(err, true)
	}

	log.Infof("recorded distinct event for %q", *event.Username)
	return nil
}

func (rt *runtime) Close() error {
	return rt.client.Close()
}

// record a daily event into Redis, storing it up to 30 days
// TODO:
//  + Ensure operation is thread-safe
//  + Merge events older then 30 days into a monthly bucket
//  + Cleanup events that have been merged into a monthly bucket
func (rt *runtime) record(event *pkg.Event) error {
	id := dailyIDFromEvent(event)
	cmd := rt.client.Incr(id)
	return cmd.Err()
}

// ensure given flags make sense
func validateFlags() error {
	if redisAddress == "" {
		return errors.New("redis instance's address not given, while this is required")
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
		log.Errorf("couldn't create redis runtime: %q", err)
	}
	defer rt.Close()

	cfg := rpc.NewAMQPConsConfig().WithName("distinctName")
	consumer, err := rpc.NewAMQPConsumer(cfg)
	if err != nil {
		log.Errorf("couldn't create consumer: %q", err)
	}
	defer consumer.Close()

	// Listen & Consume Loop
	consumer.ListenAndConsume(rt.Consume)
}

func init() {
	flag.StringVar(&redisAddress, "address", "localhost:6379", "redis instance address")
	flag.StringVar(&redisPassword, "password", "", "redis instance password")
	flag.IntVar(&redisDB, "db", 0, "redis instance db")
}
