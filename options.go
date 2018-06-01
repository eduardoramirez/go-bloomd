package bloomd

import "time"

// Option is configuration setting for the bloomD client.
type Option func(*options)

type options struct {
	hashKeys           bool
	initialConnections int
	maxAttempts        int
	maxConnections     int
	timeout            time.Duration
}

var defaultOptions = &options{
	initialConnections: defaultInitialConnections,
	hashKeys:           defaultHashKeys,
	maxAttempts:        defaultMaxAttempts,
	maxConnections:     defaultMaxConnections,
	timeout:            defaultTimeout,
}

func evaluateOptions(opts []Option) *options {
	optCopy := &options{}
	*optCopy = *defaultOptions
	for _, o := range opts {
		o(optCopy)
	}
	return optCopy
}

// WithHashKeys forces keys to be hashed before being sent to the bloomD.
func WithHashKeys(hashKeys bool) Option {
	return func(o *options) {
		o.hashKeys = hashKeys
	}
}

// WithInitialConnections sets the number of connections the pool will be
// initialized with.
func WithInitialConnections(initialConnections int) Option {
	return func(o *options) {
		o.initialConnections = initialConnections
	}
}

// WithMaxAttempts sets the number of retries the client will do if an error
// occurs when communicating with bloomD.
func WithMaxAttempts(maxAttempts int) Option {
	return func(o *options) {
		o.maxAttempts = maxAttempts
	}
}

// WithMaxConnections sets the number of maximum connections the pool will have
// at any given time.
func WithMaxConnections(maxConnections int) Option {
	return func(o *options) {
		o.maxConnections = maxConnections
	}
}

// WithTimeout sets how long the client will wait for bloomD before returning an error.
func WithTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.timeout = timeout
	}
}
