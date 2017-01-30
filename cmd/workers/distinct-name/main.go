package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"

	"gopkg.in/redis.v5"
)

// cmd redis-specific flags
var (
	redisAddress  = flag.String("address", "localhost:6379", "redis instance address")
	redisPassword = flag.String("password", "", "redis instance password")
	redisDB       = flag.Int("db", 0, "redis instance db")
)

// event struct as this worker expects it
type dailyEvent struct {
	Username *string    `json:"username"`
	Metric   *string    `json:"metric"`
	Date     *time.Time `json:"date"`
}

// Validate the daily event
func (e *dailyEvent) Validate() error {
	if e.Username == nil {
		return errors.New("required username property not given")
	}
	if e.Metric == nil {
		return errors.New("required metric property not given")
	}
	if e.Date == nil {
		return errors.New("required date property not given")
	}

	return nil
}

// Key returns the daily event data as a key
// using the format: MM.DD.<METRIC>.<USERNAME>
func (e *dailyEvent) Key() string {
	return fmt.Sprintf("%02d.%02d.%s.%s",
		e.Date.Month(), e.Date.Day(), *e.Username, *e.Metric)
}

// create a new redis runtime client
func newRuntime() (*runtime, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     *redisAddress,
		Password: *redisPassword,
		DB:       *redisDB,
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
func (rt *runtime) Consume(raw []byte) *rpc.ConsumeError {
	var event dailyEvent
	// try to unpack raw data as a daily event
	if err := rpc.UnmarshalConsumption(raw, &event); err != nil {
		return err
	}

	// validate data
	if err := event.Validate(); err != nil {
		// requeue is not required as this data is not valid
		return rpc.NewConsumeError(err, false)
	}

	if err := rt.record(&event); err != nil {
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
//  + Merge events older then 30 days into a monthly bucket
//  + Cleanup events that have been merged into a monthly bucket
func (rt *runtime) record(event *dailyEvent) error {
	cmd := rt.client.Incr(event.Key())
	return cmd.Err()
}

func main() {
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
	flag.Parse()
	log.Init()
}
