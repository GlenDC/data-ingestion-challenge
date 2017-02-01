package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"

	"expvar"
	_ "expvar"

	"github.com/glendc/data-ingestion-challenge/pkg"
	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/metrics"
	"github.com/glendc/data-ingestion-challenge/pkg/rpc"
)

// Metric-Collector Specific Flags
// see: init function for more information about each flag
var (
	port               int
	responseBufferSize int
	requestBufferSize  int
)

// rawEvent is the structure we expect as incoming data of this Metric-Collector
type rawEvent struct {
	Username string `json:"username"`
	Metric   string `json:"metric"`
	Count    int64  `json:"count"`
}

func processRequest(r *http.Request) (*pkg.Event, error) {
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

	timestamp := time.Now().UTC().Unix()
	return &pkg.Event{
		Username:  &event.Username, // required
		Metric:    &event.Metric,   // required
		Count:     &event.Count,    // required
		Timestamp: &timestamp,      // required
	}, nil
}

// ensure given flags make sense
func validateFlags() error {
	if port < 0 {
		return fmt.Errorf(
			"%d is an invalid port, should be a positive number", port)
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

	// create a worker that is used
	// to help track the metrics this running server
	serverMetrics, err := metrics.NewServer(
		metrics.DefaultServerConfig().
			WithRequestBufferSize(requestBufferSize).
			WithResponseBufferSize(responseBufferSize))
	if err != nil {
		log.Errorf("couldn't create server metrics: %q", err)
	}

	// expose (publish) any custom expvars we care about
	expvar.Publish("serverMetrics", serverMetrics)

	// spawn coroutine this metrics worker can use to wait and listen
	go serverMetrics.ListenAndCompute()

	// create RPC producer, used to dispatch our events to
	producer, err := rpc.NewAMQPProducer()
	if err != nil {
		log.Errorf("couldn't create amqp producer: %q", err)
	}
	defer producer.Close()

	http.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			serverMetrics.Request(r, start, false)
			return
		}

		event, err := processRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			serverMetrics.Request(r, start, false)
			return
		}

		if err = producer.Dispatch(event); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			serverMetrics.Request(r, start, false)
			return
		}

		serverMetrics.Request(r, start, true)
	})

	log.Infof("Metric Collector Service listening to port %d", port)
	uri := fmt.Sprintf(":%d", port)
	if err := http.ListenAndServe(uri, http.DefaultServeMux); err != nil {
		log.Errorf("couldn't start metric collector service: %q", err)
	}
}

func init() {
	// register metric-collector specific flag(s)
	flag.IntVar(&port, "port", 3000,
		"port the metric collector service will listen to")
	flag.IntVar(&responseBufferSize, "resp-buffer", 256,
		"amount of responseTimes to cache, used to compute the avg resp time")
	flag.IntVar(&requestBufferSize, "req-buffer", 1024,
		"amount of requests that can wait in line to be tracked before the server is blocked")
}
