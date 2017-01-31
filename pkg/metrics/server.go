package metrics

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/glendc/data-ingestion-challenge/pkg/log"
)

// DefaultServerConfig creates a server config with sane defaults
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		ResponseBufferSize: 256,
		RequestBufferSize:  1024,
	}
}

// ServerConfig is used to configure a server metrics worker
type ServerConfig struct {
	ResponseBufferSize int
	RequestBufferSize  int
}

// WithResponseBufferSize sets the response buffer size configuration
// and returns the updated version of itself
func (cfg *ServerConfig) WithResponseBufferSize(size int) *ServerConfig {
	cfg.ResponseBufferSize = size
	return cfg
}

// WithRequestBufferSize sets the request buffer size configuration
// and returns the updated version of itself
func (cfg *ServerConfig) WithRequestBufferSize(size int) *ServerConfig {
	cfg.RequestBufferSize = size
	return cfg
}

// validate the ServerConfig properties
func (cfg *ServerConfig) validate() error {
	if cfg.ResponseBufferSize < 8 {
		return fmt.Errorf(
			"%d is an invalid ResponseBufferSize, should be at least 8",
			cfg.ResponseBufferSize)
	}
	if cfg.RequestBufferSize < 1 {
		return fmt.Errorf(
			"%d is an invalid RequestBufferSize, should be at least 1",
			cfg.RequestBufferSize)
	}
	return nil
}

// Server collect all metrics we want to track about a server
type Server struct {
	requests       uint64
	failedRequests uint64

	minRespTime         time.Duration
	maxRespTime         time.Duration
	respTimeBuffer      []time.Duration
	respTimeBufferIndex int
	respStartTime       time.Time

	mtx sync.Mutex
	// constants given by user

	ch chan serverInput
	// constants given by user
	cfg *ServerConfig
}

// NewServer creates a metrics worker that is meant
// to track and compute the metrics of an active server
func NewServer(cfg *ServerConfig) (*Server, error) {
	if cfg == nil {
		cfg = DefaultServerConfig()
	} else if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("can't create server as config is invalid: %q", err)
	}

	return &Server{
		minRespTime:    time.Duration(math.MaxInt64),
		maxRespTime:    time.Duration(math.MinInt64),
		respTimeBuffer: make([]time.Duration, cfg.ResponseBufferSize),
		ch:             make(chan serverInput, cfg.RequestBufferSize),
		cfg:            cfg,
	}, nil
}

// ListenAndCompute listens for incoming track requests,
// it computes all requests in the order they come in.
func (s *Server) ListenAndCompute() {
	log.Infof("Server Metrics Worker up and running! Waiting for requests to come in...")
	for {
		select {
		case input := <-s.ch:
			s.trackRequest(&input)
		}
	}
}

// Request notifies the Server Metrics worker
// about a new request, which info it will track ASAP
func (s *Server) Request(r *http.Request, start time.Time, success bool) {
	s.ch <- serverInput{
		RespTime: time.Now().Sub(start),
		Success:  success,
	}
}

// String returns the server metrics as a valid JSON Object,
// implementing the expvar.Var interface
func (s *Server) String() string {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// early return in case no requests have been progressed yet,
	// as all the other data is pretty useless if so
	if s.requests == 0 {
		return `{
			"requests": {
				"total": 0
			}
		}`
	}

	successRequests := s.requests - s.failedRequests
	avgRespTime := s.computeAverageRespTime()

	return fmt.Sprintf(`{
		"requests": {
			"total": %d,
			"failed": %d,
			"successfull": %d,
		},
		"responseTimes": {
			"minimum": %f,
			"maximum": %f,
			"average": %f,
		},
	}`,
		s.requests,
		s.failedRequests,
		successRequests,
		s.minRespTime.Seconds(),
		s.maxRespTime.Seconds(),
		avgRespTime.Seconds(),
	)
}

// server metric input
type serverInput struct {
	RespTime time.Duration
	Success  bool
}

// track a request, for most data we only care about successfull requests
func (s *Server) trackRequest(in *serverInput) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.requests++

	// if the request was not successfull we don't compute response times
	// as this would skew the data, knowing that failed requests
	// are much quicker to process in many cases
	// (only when RabbitMQ fails this wouldn't be the case)
	if !in.Success {
		s.failedRequests++
		return
	}

	// compute min/max resp time values
	if in.RespTime < s.minRespTime {
		s.minRespTime = in.RespTime
	}
	if in.RespTime > s.maxRespTime {
		s.maxRespTime = in.RespTime
	}

	// store resp time, as this can be used to compute the average
	index := s.respTimeBufferIndex % s.cfg.ResponseBufferSize
	s.respTimeBuffer[index] = in.RespTime
	s.respTimeBufferIndex++
}

// computeAverageRespTime computes the average response time
// it does so by taking into account all last N response times
// where N is the number equal to the size of the buffer, capped at a maximum.
func (s *Server) computeAverageRespTime() time.Duration {
	var avgRespTime time.Duration
	respSize := s.cfg.ResponseBufferSize
	if uint64(respSize) > s.requests {
		respSize = int(s.requests)
	}

	respIndex := s.respTimeBufferIndex - respSize
	if respIndex < 0 {
		respIndex = respSize + respIndex
	}

	for i := 0; i < respSize; i++ {
		avgRespTime += s.respTimeBuffer[respIndex%s.cfg.ResponseBufferSize]
		respIndex++
	}

	return avgRespTime / time.Duration(respSize)
}
