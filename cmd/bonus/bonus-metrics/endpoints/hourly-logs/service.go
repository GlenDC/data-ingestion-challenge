package hourlylogs

import (
	"net/http"
	"strings"

	"github.com/glendc/data-ingestion-challenge/cmd/bonus/bonus-metrics/endpoints"
)

type service struct {
	rt *runtime
}

// NewService creates a new hourly-logs service
func NewService() (endpoints.Service, error) {
	rt, err := newRuntime()
	if err != nil {
		return nil, err
	}

	return &service{
		rt: rt,
	}, nil
}

// Serve the /hourly_logs endpoint
func (s *service) Serve(path string, w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet { // this service only handles get endpoints
		http.NotFound(w, r)
		return false
	}

	switch strings.TrimSuffix(path, "/") {
	case "total":
		result, err := s.rt.GetTotalMetrics()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return false
		}
		return endpoints.WriteJSON(result, w)

	case "per_user":
		result, err := s.rt.GetPerUserMetrics()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return false
		}
		return endpoints.WriteJSON(result, w)
	}

	http.NotFound(w, r)
	return false
}

// Close any open mongodb connections
func (s *service) Close() error {
	return s.rt.Close()
}
