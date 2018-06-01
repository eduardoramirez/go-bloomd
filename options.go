package bloomd

import "time"

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
}

func evaluateOptions(opts []Option) *options {
	optCopy := &options{}
	*optCopy = *defaultOptions
	for _, o := range opts {
		o(optCopy)
	}
	return optCopy
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.timeout = timeout
	}
}

func WithMaxAttempts(maxAttempts int) Option {
	return func(o *options) {
		o.maxAttempts = maxAttempts
	}
}

func WithHashKeys(hashKeys bool) Option {
	return func(o *options) {
		o.hashKeys = hashKeys
	}
}
