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
	uuid       string
	version    *int
	gtid       string // Requested GTID position
	bufferSize int
	logger     logger
}

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
func WithAuth(user, password string) func(*Options) {
	return func(o *Options) {
		o.auth.user = user
		o.auth.password = password
	}
}

// WithGTID sets the GTID position.
func WithGTID(gtid string) func(*Options) {
	return func(o *Options) {
		o.gtid = gtid
	}
}

// WithTimeout sets connection timeouts.
func WithTimeout(readTimeout, writeTimeout time.Duration) func(*Options) {
	return func(o *Options) {
		o.timeouts.read = readTimeout
		o.timeouts.write = writeTimeout
	}
}

// WithUUID sets the UUID of the client.
func WithUUID(uuid string) func(*Options) {
	return func(o *Options) {
		o.uuid = uuid
	}
}

// WithVersion sets the binlog version to use.
func WithVersion(version int) func(*Options) {
	return func(o *Options) {
		o.version = &version
	}
}

// WithBufferSize sets the buffer size for the data stream.
func WithBufferSize(bufferSize int) func(*Options) {
	return func(o *Options) {
		o.bufferSize = bufferSize
	}
}

// WithLogger sets the logger to report issues when processing the data stream.
func WithLogger(logger logger) func(*Options) {
	return func(o *Options) {
		o.logger = logger
	}
}

type logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}
