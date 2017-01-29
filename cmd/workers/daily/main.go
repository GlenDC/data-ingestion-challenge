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

// Record a daily event into Redis, storing it up to 30 days
func (rt *runtime) Record(event *dailyEvent) error {
	key := event.Key()
	log.Infof("incrementing counter of %q", key)
	cmd := rt.client.Incr(key)
	return cmd.Err()
}

func main() {
	rt, err := newRuntime()
	if err != nil {
		log.Errorf("couldn't create redis runtime: %q", err)
	}

	cfg := rpc.NewConsumerConfig().WithName("daily")
	rpc.Consumer(cfg, func(ch rpc.DeliveryChannel) {
		forever := make(chan bool)

		go func() {
			var event dailyEvent
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

		log.Infof("Tracking daily distinct events. To exit press CTRL+C")
		<-forever
	})
}

func init() {
	flag.Parse()
	log.Init()
}
