package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/glendc/data-ingestion-challenge/pkg"
	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// cmd mongo-specific flags
// see: init function for more information about each flag
var (
	mgoAddress    string
	mgoDatabase   string
	mgoCollection string
	gcInterval    time.Duration
	gcDisabled    bool
)

// defines how long a record stored in this hourlyLog actually lives,
// before it gets wiped off by the garbage collector
const recordTTL = time.Hour

var cleanupSelector = bson.M{
	pkg.EventTimestampID: bson.M{
		// event is older than 1 hour (thus less than now - 1h)
		"$lt": time.Now().UTC().Add(recordTTL * -1).Unix(),
	},
}

func newRuntime() (*runtime, error) {
	session, err := mgo.Dial(mgoAddress)
	if err != nil {
		return nil, fmt.Errorf("couldn't open mongo session: %q", err)
	}
	session.SetMode(mgo.Monotonic, true)

	if err = session.Ping(); err != nil {
		return nil, fmt.Errorf("couldn't ping mongo server: %q", err)
	}

	return &runtime{
		session: session,
	}, nil
}

type runtime struct {
	session *mgo.Session
}

// Consume raw event data and store it as an anonymous object into mongodb
// Validation of the actual data is not done in this worker
func (rt *runtime) Consume(event *pkg.Event) *rpc.ConsumeError {
	if err := rt.record(event); err != nil {
		// requeue is required as this is a mistake on our part
		// perhaps another accountName worker can handle this
		// or we can try again later
		return rpc.NewConsumeError(err, true)
	}

	log.Infof("recorded event for up to 1 hour")
	return nil
}

// RemoveOldLogs, which are logs older then 1 hour
func (rt *runtime) RemoveOldLogs() error {
	collection, err := rt.getCollection()
	if err != nil {
		return fmt.Errorf("couldn't find collection: %q", err)
	}

	resp, err := collection.RemoveAll(cleanupSelector)
	if err != nil {
		return err
	}

	log.Infof("removed %d hourly logs", resp.Removed)
	return nil
}

func (rt *runtime) Close() error {
	rt.session.Close()
	return nil
}

// record event for 1 hour in MongoDB
func (rt *runtime) record(event *pkg.Event) error {
	collection, err := rt.getCollection()
	if err != nil {
		return fmt.Errorf("couldn't find collection: %q", err)
	}

	// insert event into collection
	return collection.Insert(event)
}

func (rt *runtime) getCollection() (*mgo.Collection, error) {
	db := rt.session.DB(mgoDatabase)
	if db == nil {
		return nil, fmt.Errorf("no mongo database could be found for %q", mgoDatabase)
	}
	collection := db.C(mgoCollection)
	if collection == nil {
		return nil, fmt.Errorf("no collection named %q could be found in %q",
			mgoCollection, mgoDatabase)
	}

	return collection, nil
}

// ensure given flags make sense
func validateFlags() error {
	if mgoAddress == "" {
		return errors.New("mongo instance's address not given, while this is required")
	}
	if mgoDatabase == "" {
		return errors.New("mongo database's name not given, while this is required")
	}
	if mgoCollection == "" {
		return errors.New("mongodb collection's name not given, while this is required")
	}
	if gcInterval <= 0 {
		return errors.New("garbage collector's interval has to be positive and non-zero")
	}
	return nil
}

// cleanupJob is a seperate coroutine, running just a cleanup job
//
// NOTE: another cleanup approach that has been considered is cleaning while inserting,
// and thus piggy-backing on the existing program flow,
// while this does make the program slightly simpler, it did seem like
// it would make the program more expensive, without much simplicity to be gained
func cleanupJob(rt *runtime) {
	log.Infof("clean up job up and running, removing old logs every %v", gcInterval)
	var gcError error

	for {
		if gcError = rt.RemoveOldLogs(); gcError != nil {
			log.Warningf("couldn't cleanup old hourly logs: %q", gcError)
		}
		time.Sleep(gcInterval)
	}
}

func main() {
	flag.Parse() // parse all (non-)specific flags
	err := validateFlags()
	if err != nil {
		flag.Usage()
		log.Errorf("invalid flag: %q", err)
	}

	// create runtime that we'll use as a consumer
	rt, err := newRuntime()
	if err != nil {
		log.Errorf("couldn't create mongo runtime: %q", err)
	}
	defer rt.Close()

	// cleanup job
	if !gcDisabled {
		go cleanupJob(rt)
	}

	cfg := rpc.NewAMQPConsConfig().WithName("hourlyLog")
	consumer, err := rpc.NewAMQPConsumer(cfg)
	if err != nil {
		log.Errorf("couldn't create consumer: %q", err)
	}
	defer consumer.Close()

	// Listen & Consume Loop
	consumer.ListenAndConsume(rt.Consume)
}

func init() {
	flag.StringVar(&mgoAddress, "address", "localhost:27017", "mongo instance address")
	flag.StringVar(&mgoDatabase, "db", "events", "mongo database")
	flag.StringVar(&mgoCollection, "collection", "hourly", "mongo db collection")
	flag.DurationVar(&gcInterval, "gc-interval", time.Minute*30,
		"Garbage Collector interval on which it deletes old logs")
	flag.BoolVar(&gcDisabled, "disable-gc", false,
		"disable the async worker responsible for cleaning up logs older than 1 hour")
}
