package main

import (
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"expvar"
	_ "expvar"

	"github.com/glendc/data-ingestion-challenge/cmd/bonus/bonus-metrics/endpoints"
	"github.com/glendc/data-ingestion-challenge/cmd/bonus/bonus-metrics/endpoints/hourly-logs"
	"github.com/glendc/data-ingestion-challenge/pkg/log"
	"github.com/glendc/data-ingestion-challenge/pkg/metrics"
)

// Metric-Collector Specific Flags
// see: init function for more information about each flag
var (
	port               int
	responseBufferSize int
	requestBufferSize  int
)

// ensure given flags make sense
func validateFlags() error {
	if port < 0 {
		return fmt.Errorf(
			"%d is an invalid port, should be a positive number", port)
	}

	// if our flags are correct we can check our submodule flags
	return hourlylogs.ValidateFlags()
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

	// collects all services
	services := make(map[string]endpoints.Service)
	// hourly-log endpoint
	services["hourly_logs"], err = hourlylogs.NewService()
	if err != nil {
		log.Errorf("couldn't create hourly_logs metrics service: %q", err)
	}
	defer services["hourly_logs"].Close()

	for path, service := range services {
		path = fmt.Sprintf("/metrics/%s/", path)
		log.Infof("Creating handler for %s", path)
		http.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			p := strings.TrimPrefix(r.URL.Path, path)
			success := service.Serve(p, w, r)
			serverMetrics.Request(r, start, success)
		})
	}

	log.Infof("Bonus Metrics Service listening to port %d", port)
	uri := fmt.Sprintf(":%d", port)
	if err := http.ListenAndServe(uri, http.DefaultServeMux); err != nil {
		log.Errorf("couldn't start bonus metrics service: %q", err)
	}
}

func init() {
	// register metric-collector specific flag(s)
	flag.IntVar(&port, "port", 3000,
		"port the bonus metrics service will listen to")
	flag.IntVar(&responseBufferSize, "resp-buffer", 256,
		"amount of responseTimes to cache, used to compute the avg resp time")
	flag.IntVar(&requestBufferSize, "req-buffer", 1024,
		"amount of requests that can wait in line to be tracked before the server is blocked")
}
