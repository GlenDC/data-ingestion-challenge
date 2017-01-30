package rpc

// Producer defines an interface for the producing side of an RPC system
// It's only required functionality is that it can dispatch data
type Producer interface {
	// Close any open connections and clean up
	Close() error
	// Dispatch data over an open connection
	Dispatch(data interface{}) error
}

// NewConsumeError creates an error that has happened during consumption
func NewConsumeError(err error, requeue bool) *ConsumeError {
	return &ConsumeError{
		Requeue: requeue,
		err:     err,
	}
}

// ConsumeError is a specific error
// that is used by the ConsumeCallback of a Consumer implementation
// Besides containing an error it also defines if the data should be requeued
type ConsumeError struct {
	Requeue bool
	err     error
}

// Error returns the actual error that caused the consumption to fail
func (e *ConsumeError) Error() string {
	return e.err.Error()
}

// ConsumeCallback is the function that is to be used by actual consumers,
// to process the data and use it for a practical purpose.
type ConsumeCallback func(raw []byte) *ConsumeError

// Consumer defines an interface on the consumption side of an RPC system
// It's only required functionality is that it Listens to an open connection
// and passes on the data it receives to the ConsumeCallback
type Consumer interface {
	// Close any open connections and clean up
	Close() error
	// Listen to an open connection and Consume the data via the given callback
	ListenAndConsume(ConsumeCallback)
}
