package gomaxscale

import (
	"log"
	"time"
)

// Options stores all available options to connect with MaxScale.
type Options struct {
	auth struct {
		user     string
		password string
	}
	timeouts struct {
		timeRef func() time.Time
		read    time.Duration
		write   time.Duration
	}
	stats struct {
		period time.Duration
		ticker func(Stats)
	}
	uuid       string
	version    *int
	gtid       string // Requested GTID position
	bufferSize int
	logger     logger
}

// Option is a function that can be used to configure the library.
type Option func(*Options)

func newDefaultOptions() Options {
	var opts Options
	opts.uuid = "XXX-YYY_YYY"
	opts.timeouts.read = 2 * time.Second
	opts.timeouts.write = 2 * time.Second
	opts.timeouts.timeRef = time.Now
	opts.bufferSize = 4096
	opts.logger = log.Default()
	return opts
}

// WithAuth sets the authentication options.
func WithAuth(user, password string) Option {
	return func(o *Options) {
		o.auth.user = user
		o.auth.password = password
	}
}

// WithGTID sets the GTID position.
func WithGTID(gtid string) Option {
	return func(o *Options) {
		o.gtid = gtid
	}
}

// WithTimeout sets connection timeouts.
func WithTimeout(readTimeout, writeTimeout time.Duration) Option {
	return func(o *Options) {
		o.timeouts.read = readTimeout
		o.timeouts.write = writeTimeout
	}
}

// WithStats enables statistics in the library. The ticker callback is called
// every period of time with information about events and processing time.
func WithStats(period time.Duration, ticker func(Stats)) Option {
	return func(o *Options) {
		o.stats.period = period
		o.stats.ticker = ticker
	}
}

// WithUUID sets the UUID of the client.
func WithUUID(uuid string) Option {
	return func(o *Options) {
		o.uuid = uuid
	}
}

// WithVersion sets the binlog version to use.
func WithVersion(version int) Option {
	return func(o *Options) {
		o.version = &version
	}
}

// WithBufferSize sets the buffer size for the data stream.
func WithBufferSize(bufferSize int) Option {
	return func(o *Options) {
		o.bufferSize = bufferSize
	}
}

// WithLogger sets the logger to report issues when processing the data stream.
func WithLogger(logger logger) Option {
	return func(o *Options) {
		o.logger = logger
	}
}

type logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}
