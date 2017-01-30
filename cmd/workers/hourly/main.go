package main

import (
	"flag"

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

func main() {
	session, err := mgo.Dial(*mgoAddress)
	if err != nil {
		log.Errorf("couldn't open mongo session: %q", err)
	}
	defer session.Close()

	db := session.DB(*mgoDatabase)
	if db == nil {
		log.Errorf("no mongo database could be found for %q", *mgoDatabase)
	}

	collection := db.C(*mgoCollection)
	if collection == nil {
		log.Errorf("no collection named %q could be found in %q",
			*mgoCollection, *mgoDatabase)
	}

	cfg := rpc.NewConsumerConfig().WithName("hourly")
	rpc.Consumer(cfg, func(ch rpc.DeliveryChannel) {
		forever := make(chan bool)

		go func() {
			var event hourlyEvent
			var err error

			for d := range ch {
				// process delivery
				if err = rpc.Consume(d, &event); err != nil {
					log.Warningf("event was rejected: %q", err)
					continue
				}

				// track event and acknowledge if all is OK
				if err = collection.Insert(event); err != nil {
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
