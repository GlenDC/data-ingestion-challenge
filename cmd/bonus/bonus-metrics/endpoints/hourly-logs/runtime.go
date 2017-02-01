package hourlylogs

import (
	"errors"
	"flag"
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// cmd mongodb-specific flags
var (
	mgoAddress    string
	mgoDatabase   string
	mgoCollection string
)

func newRuntime() (*runtime, error) {
	session, err := mgo.Dial(mgoAddress)
	if err != nil {
		return nil, fmt.Errorf("couldn't open mongo session: %q", err)
	}
	session.SetMode(mgo.Monotonic, true)

	return &runtime{
		session: session,
	}, nil
}

type runtime struct {
	session *mgo.Session
}

// GetPerUserMetrics computed using MongoDB aggregations
// live from the specified document (collection)
func (rt *runtime) GetPerUserMetrics() (interface{}, error) {
	collection, err := rt.getCollection()
	if err != nil {
		return nil, fmt.Errorf("couldn't get collection: %q", err)
	}

	pipe := collection.Pipe([]bson.M{
		bson.M{ // first and only stage: group all metrics together
			"$group": bson.M{
				"_id": bson.M{
					"username": "$username",
					"metric":   "$metric",
				},
				"minimum": bson.M{"$min": "$count"},
				"maximum": bson.M{"$max": "$count"},
				"average": bson.M{"$avg": "$count"},
				"count":   bson.M{"$sum": 1},
			},
		},
	})
	iter := pipe.Iter()
	if err = iter.Err(); err != nil {
		return nil, fmt.Errorf("couldn't aggregate collection: %q", err)
	}

	var all []map[string]interface{}
	if err = iter.All(&all); err != nil {
		return nil, fmt.Errorf("couldn't unpack aggregation result: %q", err)
	}

	return all, nil
}

// GetTotalMetrics computed using MongoDB aggregations
// live from the specified document (collection)
func (rt *runtime) GetTotalMetrics() (interface{}, error) {
	collection, err := rt.getCollection()
	if err != nil {
		return nil, fmt.Errorf("couldn't get collection: %q", err)
	}

	pipe := collection.Pipe([]bson.M{
		bson.M{ // first and only stage: group all metrics together
			"$group": bson.M{
				"_id":     "$metric",
				"minimum": bson.M{"$min": "$count"},
				"maximum": bson.M{"$max": "$count"},
				"average": bson.M{"$avg": "$count"},
				"count":   bson.M{"$sum": 1},
			},
		},
	})
	iter := pipe.Iter()
	if err = iter.Err(); err != nil {
		return nil, fmt.Errorf("couldn't aggregate collection: %q", err)
	}

	// remap result so that we have a per-id map
	var all []map[string]interface{}
	if err = iter.All(&all); err != nil {
		return nil, fmt.Errorf("couldn't unpack aggregation result: %q", err)
	}

	return all, nil
}

func (rt *runtime) Close() error {
	rt.session.Close()
	return nil
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

// ValidateFlags ensures given flags make sense
func ValidateFlags() error {
	if mgoAddress == "" {
		return errors.New("mongo instance's address not given, while this is required")
	}
	if mgoDatabase == "" {
		return errors.New("mongo database's name not given, while this is required")
	}
	if mgoCollection == "" {
		return errors.New("mongodb collection's name not given, while this is required")
	}
	return nil
}

func init() {
	flag.StringVar(&mgoAddress, "mgo-address", "localhost:27017", "mongo instance address")
	flag.StringVar(&mgoDatabase, "mgo-db", "events", "mongo database")
	flag.StringVar(&mgoCollection, "mgo-collection", "hourly", "mongo db collection")
}
