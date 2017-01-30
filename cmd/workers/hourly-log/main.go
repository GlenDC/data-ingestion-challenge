package main

import (
	"flag"
	"fmt"

	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"

	"gopkg.in/mgo.v2"
)

// cmd redis-specific flags
var (
	mgoAddress    = flag.String("address", "localhost:27017", "mongo instance address")
	mgoDatabase   = flag.String("db", "events", "mongo database")
	mgoCollection = flag.String("collection", "hourly", "mongo db collection")
)

// For this worker we don't care so much about the specifics of what makes up an event,
// as long as it is valid according to the metric collector service,
// this worker will accept it as well.
type hourlyEvent map[string]interface{}

func newRuntime() (*runtime, error) {
	session, err := mgo.Dial(*mgoAddress)
	if err != nil {
		return nil, fmt.Errorf("couldn't open mongo session: %q", err)
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
func (rt *runtime) Consume(raw []byte) *rpc.ConsumeError {
	var event hourlyEvent
	// try to unpack raw data as a an hourly event
	if err := rpc.UnmarshalConsumption(raw, &event); err != nil {
		return err
	}

	if err := rt.record(event); err != nil {
		// requeue is required as this is a mistake on our part
		// perhaps another accountName worker can handle this
		// or we can try again later
		return rpc.NewConsumeError(err, true)
	}

	log.Infof("recorded event for up to 1 hour")
	return nil
}

func (rt *runtime) Close() error {
	rt.session.Close()
	return nil
}

// record event for 1 hour in MongoDB
func (rt *runtime) record(event hourlyEvent) error {
	// get collection
	db := rt.session.DB(*mgoDatabase)
	if db == nil {
		return fmt.Errorf("no mongo database could be found for %q", *mgoDatabase)
	}
	collection := db.C(*mgoCollection)
	if collection == nil {
		return fmt.Errorf("no collection named %q could be found in %q",
			*mgoCollection, *mgoDatabase)
	}

	// insert event into collection
	// TODO
	//   + Ensure event is only stored for 1 hour!
	return collection.Insert(event)
}

func main() {
	rt, err := newRuntime()
	if err != nil {
		log.Errorf("couldn't create mongo runtime: %q", err)
	}
	defer rt.Close()

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
	flag.Parse()
	log.Init()
}
