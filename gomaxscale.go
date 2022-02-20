package gomaxscale

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

const initBufferSize = 1024

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

// Consumer is a Go implementation of a MaxScale CDC consumer.
type Consumer struct {
	address  string
	database string
	table    string
	options  Options

	events chan Event
	done   chan bool
}

// NewConsumer creates a new Consumer instance.
func NewConsumer(address, database, table string, optFuncs ...func(*Options)) *Consumer {
	var opts Options
	opts.uuid = "XXX-YYY_YYY"
	opts.timeouts.read = 2 * time.Second
	opts.timeouts.write = 2 * time.Second
	opts.timeouts.timeRef = time.Now
	opts.bufferSize = 4096
	opts.logger = log.Default()

	for _, f := range optFuncs {
		f(&opts)
	}
	return &Consumer{
		address:  address,
		database: database,
		table:    table,
		options:  opts,
	}
}

// Start connects to MaxScale and starts consuming events.
//
// https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/
func (g *Consumer) Start() (*DataStream, error) {
	g.options.logger.Print("⚠️  This is an older version of the library with some protocol misconceptions. " +
		"Please upgrade to a newer version.")

	// if the consumer is already running, close it before starting a new one
	if g.done != nil {
		g.Close()
	}

	g.events = make(chan Event)
	g.done = make(chan bool)

	conn, err := net.Dial("tcp", g.address)
	if err != nil {
		return nil, err
	}

	//
	//  Authentication
	//

	var auth bytes.Buffer
	if _, err := auth.WriteString(g.options.auth.user); err != nil {
		g.disconnect(conn)
		return nil, fmt.Errorf("failed to write username in authentication: %w", err)
	}
	if _, err := auth.WriteString(":"); err != nil {
		g.disconnect(conn)
		return nil, fmt.Errorf("failed to write separator in authentication: %w", err)
	}
	password := sha1.Sum([]byte(g.options.auth.password))
	if _, err := auth.Write(password[:]); err != nil {
		g.disconnect(conn)
		return nil, fmt.Errorf("failed to write password in authentication: %w", err)
	}
	if err := g.writeInitRequest(conn, []byte(hex.EncodeToString(auth.Bytes()))); err != nil {
		g.disconnect(conn)
		return nil, fmt.Errorf("failed to authenticate: %w", err)
	}

	//
	// Registration
	//

	if err := g.writeInitRequest(conn, []byte("REGISTER UUID="+g.options.uuid+", TYPE=JSON")); err != nil {
		g.disconnect(conn)
		return nil, fmt.Errorf("failed to register: %w", err)
	}

	//
	// Data stream
	//

	var requestDataStream bytes.Buffer
	requestDataStream.WriteString("REQUEST-DATA " + g.database + "." + g.table)
	if g.options.version != nil {
		requestDataStream.WriteString("." + strconv.FormatInt(int64(*g.options.version), 10))
	}
	if g.options.gtid != "" {
		requestDataStream.WriteString(" " + g.options.gtid)
	}
	if _, err := conn.Write(requestDataStream.Bytes()); err != nil {
		g.disconnect(conn)
		return nil, fmt.Errorf("failed to request data stream: %w", err)
	}

	stream := stream{
		conn:        conn,
		readTimeout: g.options.timeouts.read,
		timeRef:     g.options.timeouts.timeRef,
		bufferSize:  g.options.bufferSize,
	}

	//
	// Initial event
	//

	response, err := stream.scan()
	if err != nil {
		g.disconnect(conn)
		return nil, fmt.Errorf("failed to read initial data stream: %w", err)
	}
	var dataStream DataStream
	if err := json.Unmarshal(response, &dataStream); err != nil {
		g.disconnect(conn)
		return nil, fmt.Errorf("failed to decode initial data stream '%s': %w", string(response), err)
	}

	//
	// Events
	//

	go func() {
		for {
			select {
			case <-g.done:
				g.disconnect(conn)
				return

			default:
				response, err := stream.scan()
				if err != nil {
					errCause := err
					for errors.Unwrap(errCause) != nil {
						errCause = errors.Unwrap(errCause)
					}
					if errCause == io.EOF {
						g.disconnect(conn)
						return
					}
					if netErr, ok := errCause.(net.Error); !ok || !netErr.Timeout() {
						g.options.logger.Printf("failed to read from data stream: %s", err)
					}
					continue

				} else if len(response) == 0 {
					continue
				}

				var event Event
				if err := json.Unmarshal(response, &event); err != nil {
					g.options.logger.Printf("failed to decode '%s' from data stream: %s", string(response), err)
					continue
				}
				event.RawData = response
				g.events <- event
			}
		}
	}()

	return &dataStream, nil
}

// Process starts consuming events and trigerring the callback function for each
// event.
func (g *Consumer) Process(eventFunc func(Event)) {
	for event := range g.events {
		eventFunc(event)
	}
}

// Close closes the connection to MaxScale.
func (g *Consumer) Close() {
	defer func() {
		// done channel could be already closed (generating a panic) if MaxScale
		// server closed the connection
		_ = recover()
	}()

	if g.done == nil {
		return
	}

	g.done <- true
	for {
		// wait for the last event to be processed
		if _, ok := <-g.events; !ok {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (g *Consumer) writeInitRequest(conn net.Conn, b []byte) error {
	if err := conn.SetWriteDeadline(g.options.timeouts.timeRef().Add(g.options.timeouts.write)); err != nil {
		return fmt.Errorf("failed to set write timeout: %w", err)
	}

	if _, err := conn.Write(b); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	if err := conn.SetReadDeadline(g.options.timeouts.timeRef().Add(g.options.timeouts.read)); err != nil {
		return fmt.Errorf("failed to set read timeout: %w", err)
	}

	response := make([]byte, initBufferSize)
	n, err := conn.Read(response)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}
	response = response[:n]

	return checkResponseError(response)
}

func (g *Consumer) disconnect(conn net.Conn) {
	if err := conn.Close(); err != nil {
		g.options.logger.Printf("failed to close connection: %s", err)
	}

	close(g.events)
	close(g.done)
}

type stream struct {
	conn        net.Conn
	readTimeout time.Duration
	timeRef     func() time.Time

	buffer     []byte
	bufferSize int
}

func (s *stream) scan() ([]byte, error) {
	var response bytes.Buffer
	var identation int
	var loops int

	for {
		if err := s.conn.SetReadDeadline(s.timeRef().Add(s.readTimeout)); err != nil {
			return nil, fmt.Errorf("failed to set read timeout: %w", err)
		}

		tmpBuffer := make([]byte, s.bufferSize)
		n, err := s.conn.Read(tmpBuffer)
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}
		tmpBuffer = tmpBuffer[:n]

		if s.buffer != nil {
			tmpBuffer = append(s.buffer, tmpBuffer...)
			s.buffer = s.buffer[:0]
		}

		var i int
		for _, b := range tmpBuffer {
			if b == '{' {
				identation++
			} else if b == '}' {
				identation--
			}
			i++
			if identation == 0 {
				break
			}
		}

		response.Write(tmpBuffer[:i])
		if identation == 0 {
			if i < len(tmpBuffer) {
				s.buffer = tmpBuffer[i:]
			}
			break
		}

		loops++
		if loops > 100 {
			return nil, errors.New("too many network iterations to find a json object")
		}
	}
	return bytes.TrimSpace(response.Bytes()), checkResponseError(response.Bytes())
}

func checkResponseError(response []byte) error {
	if bytes.Contains(bytes.ToLower(response), []byte("err")) {
		return fmt.Errorf("error raised from maxscale: %s", string(response))
	}
	return nil
}

// DataStream is the first event response with the target table information.
//
// https://avro.apache.org/docs/1.11.0/spec.html#schemas
type DataStream struct {
	Namespace string `json:"namespace"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	Table     string `json:"table"`
	Database  string `json:"database"`
	Version   int    `json:"version"`
	GTID      string `json:"gtid"`
	Fields    []struct {
		Name     string      `json:"name"`
		Type     interface{} `json:"type"`
		RealType string      `json:"real_type"`
		Length   int         `json:"length"`
		Unsigned bool        `json:"unsigned"`
	} `json:"fields"`
}

// Event is a MaxScale event.
type Event struct {
	Domain      int    `json:"domain"`
	ServerID    int    `json:"server_id"`
	Sequence    int    `json:"sequence"`
	EventNumber int    `json:"event_number"`
	Timestamp   int64  `json:"timestamp"`
	Type        string `json:"event_type"`
	RawData     []byte `json:"-"`
}
