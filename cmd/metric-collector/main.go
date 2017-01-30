package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/streadway/amqp"

	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"
)

var (
	port = flag.Int("port", 3000, "port the metric collector service will listen to")
)

type rawEvent struct {
	Username string `json:"username,omitempty"`
	Count    int64  `json:"count,omitempty"`
	Metric   string `json:"metric,omitempty"`
}

type outputEvent struct {
	rawEvent
	Date time.Time `json:"date"`
}

func processRequest(r *http.Request) (*outputEvent, error) {
	log.Infof("processing and validating event")

	// validate content type
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		return nil, fmt.Errorf("invalid content-type %q, expected application/json", ct)
	}

	// validate types of given properties
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	var event rawEvent
	if err := decoder.Decode(&event); err != nil {
		return nil, fmt.Errorf("couldn't decode event: %q", err)
	}

	return &outputEvent{
		rawEvent: event,
		Date:     time.Now().UTC(),
	}, nil
}

func main() {
	rpc.Channel(func(ch *amqp.Channel) {
		http.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.NotFound(w, r)
				return
			}

			event, err := processRequest(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err = rpc.Dispatch(ch, event); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			log.Infof("event dispatched")
		})

		log.Infof("Metric Collector Service listening to port %d", *port)
		http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
	})
}

func init() {
	flag.Parse()
	log.Init()
}
