package endpoints

import (
	"encoding/json"
	"net/http"
)

// WriteJSON data to a response writer
// returns false in case it wrote an error instead of the data
func WriteJSON(data interface{}, w http.ResponseWriter) bool {
	bytes, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return false
	}

	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(bytes)
	return err == nil
}

// Service defines a simplistic per-endpoint service
type Service interface {
	// Serve an endpoint and return if it was successfull
	Serve(path string, w http.ResponseWriter, r *http.Request) bool
	// Close any open connections
	Close() error
}
