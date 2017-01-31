package pkg

import "errors"

// Properties of event as used in their serialized form
const (
	EventUsernameID  = "username"
	EventTimestampID = "timestamp"
	EventMetricID    = "metric"
	EventCountID     = "count"
)

// Event represents the data as passed through the metric collector-service
type Event struct {
	Username  *string `json:"username" bson:"username"`
	Timestamp *int64  `json:"timestamp" bson:"timestamp"`
	Metric    *string `json:"metric" bson:"metric"`
	Count     *int64  `json:"count" bson:"count"`
}

// Validate if all required properties are present in this event
func (e *Event) Validate() error {
	if e.Username == nil {
		return errors.New(`required username property is not present`)
	}
	if e.Timestamp == nil {
		return errors.New(`required timestamp property is not present`)
	}
	if e.Metric == nil {
		return errors.New(`required metric property is not present`)
	}
	if e.Count == nil {
		return errors.New(`required count property is not present`)
	}

	return nil
}
