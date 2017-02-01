package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/glendc/data-ingestion-challenge/pkg"
	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"

	"gopkg.in/redis.v5"
)

// cmd redis-specific flags
// see: init function for more information about each flag
var (
	redisAddress   string
	redisPassword  string
	redisDB        int
	mergeInterval  time.Duration
	mergerDisabled bool
)

// prefixes for/and keys used for redis storage
const (
	prefix       = "metrics-distinct"
	keyLastMerge = prefix + ":last-merge"
)

// we only merge events into a monthly bucket when they are older than 30 days
const mergeTimeLimitSubtractor = time.Hour * 24 * 30 * -1

// key functions to generate redis keys used for storage
func keyMonthly(date time.Time) string {
	return fmt.Sprintf("%s:%d:%02d",
		prefix, date.Year(), date.Month())
}
func keyDaily(date time.Time) string {
	return fmt.Sprintf("%s:%d:%02d:%02d",
		prefix, date.Year(), date.Month(), date.Day())
}

// create a new redis runtime client
func newRuntime() (*runtime, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: redisPassword,
		DB:       redisDB,
	})

	if _, err := client.Ping().Result(); err != nil {
		return nil, fmt.Errorf("couldn't ping redis server: %q", err)
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

	log.Infof("recorded distinct event for metric %q", *event.Metric)
	return nil
}

func (rt *runtime) Close() error {
	return rt.client.Close()
}

// used in runtime::MergeOldLogs to merge a daily bucket into a monthly bucket,
// such that both events are watched while this happens
type dailyEventMerger struct {
	monthlyBucket string
	dailyBucket   string
}

// Merge all metrics found in the given DailyBucket into the given MonthlyBucket,
// afterwards the dailyBucket gets deleted
func (m *dailyEventMerger) Merge(tx *redis.Tx) error {
	// get all its keys
	demap, err := tx.HGetAll(m.dailyBucket).Result()
	if err != nil {
		return fmt.Errorf("couldn't get daily bucket %q: %q", m.dailyBucket, err)
	}

	// a non-empty map is not an error, so we need to check if we actually have events
	// it's not considered an error if we find an empty bucket on the way,
	// as this is possible for various reasons
	// (eg. no worker was active that day to collect events)
	if len(demap) > 0 {
		log.Infof("[MERGER] merging distinct events from %q into %q",
			m.dailyBucket, m.monthlyBucket)
		// this can be done as one transaction, keys are already watched
		pipe := tx.Pipeline()
		defer pipe.Close()

		// increment all metrics in the monthly bucket
		var count int64
		for metric, rawCount := range demap {
			count, err = strconv.ParseInt(rawCount, 10, 64)
			if err != nil {
				return fmt.Errorf(
					"couldn't update metric %q (invalid count value) in bucket %q: %q",
					metric, m.monthlyBucket, err)
			}
			if err = pipe.HIncrBy(m.monthlyBucket, metric, count).Err(); err != nil {
				return fmt.Errorf(
					"couldn't update metric %q in bucket %q: %q",
					metric, m.monthlyBucket, err)
			}
		}
		// remove daily event
		if err = pipe.Del(m.dailyBucket).Err(); err != nil {
			return fmt.Errorf("couldn't delete %q: %q", m.dailyBucket, err)
		}

		// execute the actual merging
		if _, err = pipe.Exec(); err != nil {
			return fmt.Errorf("merge failed: %q", err)
		}
	}

	return nil
}

// MergeOldLogs collects all events older then 30 days,
// merges them all together in a monthly bucket,
// and deletes the original records of those events.
//
// This is a stop-the-world approach, as we need to ensure that all our commands
// are executed without interuption and without errors, or non should be executed!
// Hopefully however it should never have to merge more then a day or so,
// so it should never take too long!
//
// The merging requires maximum `3 + N * 3` transactions:
//   3 (+): Watch, Get and Set lastMerge;
//   N (*): Amount of non-empty daily buckets between lastMergeDay and 30 days ago;
//       3:  - Watch dailyKey and monthlyKey;
//           - Get all daily events;
//           - Piped Transaction with actual Merge (incl del daily bucket at the end);
//
// Meaning that if we merge every 24 hours it should only ever require 6 or 12 transactions.
func (rt *runtime) MergeOldLogs(lastMerge *redis.Tx) error {
	now := time.Now().UTC()

	// check if lastMerge is actually set
	dateRaw, err := lastMerge.Get(keyLastMerge).Result()
	if err != nil {
		return fmt.Errorf("error getting %q: %q", keyLastMerge, err)
	}
	// if empty we'll assume it's not set yet
	if dateRaw == "" {
		log.Infof("[MERGER] %q was not set yet, setting it to current time", keyLastMerge)
		// we want to store the last merge as 1 day before, as today isn't merged yet
		date := now.Add(time.Hour * -24)
		cmd := lastMerge.Set(keyLastMerge, date.Format(time.RFC1123Z), 0)
		if err = cmd.Err(); err != nil {
			return fmt.Errorf("couldn't set %q to current time: %q", keyLastMerge, err)
		}

		return nil
	}

	// parse lastMerge Date as we need it in order to merge events
	date, mergeErr := time.Parse(time.RFC1123Z, dateRaw)
	if mergeErr != nil {
		return fmt.Errorf("couldn't parse %q: %q", keyLastMerge, mergeErr)
	}
	limit := now.Add(mergeTimeLimitSubtractor - (time.Hour * 24))
	originalDate := date // so we know if date was updated

	// as long as the current lastMergeDate is older than 31 days
	for date.Before(limit) {
		// get the next non-merged day
		date = date.Add(time.Hour * 24)

		// create daily merger
		merger := dailyEventMerger{
			dailyBucket:   keyDaily(date),
			monthlyBucket: keyMonthly(date),
		}

		// watch keys & merge daily bucket into the monthly bucket
		err = rt.client.Watch(merger.Merge, merger.dailyBucket, merger.monthlyBucket)
		if err != nil {
			return fmt.Errorf("couldn't merge %q into %q: %q",
				merger.dailyBucket, merger.monthlyBucket, err)
		}
	}

	// store the updated lastMergeDate
	if date.After(originalDate) {
		log.Infof("[MERGER] updating %q to %q", keyLastMerge, date)
		cmd := lastMerge.Set(keyLastMerge, date.Format(time.RFC1123Z), 0)
		if err = cmd.Err(); err != nil {
			return fmt.Errorf("couldn't set %q to the %v: %q", keyLastMerge, date, err)
		}
	} else {
		log.Infof("[MERGER] no events older than 30 days found, nothing to do here")
	}

	return nil
}

// record a daily event into Redis, storing it up to 30 days
func (rt *runtime) record(event *pkg.Event) error {
	date := time.Unix(*event.Timestamp, 0).UTC()
	cmd := rt.client.HIncrBy(keyDaily(date), *event.Metric, 1)
	return cmd.Err()
}

// ensure given flags make sense
func validateFlags() error {
	if redisAddress == "" {
		return errors.New("redis instance's address not given, while this is required")
	}
	return nil
}

// mergeJob is a seperate coroutine, running just a merge (cleanup) job
func mergeJob(rt *runtime) {
	log.Infof("[MERGER] merge job up and running, merging old logs every %v", mergeInterval)
	var mError error

	for {
		mError = rt.client.Watch(rt.MergeOldLogs, keyLastMerge)
		if mError != nil {
			log.Warningf("[MERGER] couldn't merge monthly logs: %q", mError)
		}
		time.Sleep(mergeInterval)
	}
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

	// spawn merge job as coroutine
	// collecting logs older then 30 days and merging them into a single bucket
	if !mergerDisabled {
		go mergeJob(rt)
	}

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
	flag.DurationVar(&mergeInterval, "merge-interval", time.Hour*24,
		"Merge time interval on which it merges logs older then 30 days into a monthly bucket")
	flag.BoolVar(&mergerDisabled, "disable-merger", false,
		"don't run the async merger responsible for merging logs older than 30 days")
}
