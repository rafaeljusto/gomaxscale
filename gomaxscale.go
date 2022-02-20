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
	"regexp"
	"strconv"
	"time"
)

const initBufferSize = 1024

var (
	reDDLEvent = regexp.MustCompile(`{"namespace":`)
	reDMLEvent = regexp.MustCompile(`{"domain":`)
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

	events chan CDCEvent
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
func (g *Consumer) Start() error {
	// if the consumer is already running, close it before starting a new one
	if g.done != nil {
		g.Close()
	}

	g.events = make(chan CDCEvent)
	g.done = make(chan bool)

	conn, err := net.Dial("tcp", g.address)
	if err != nil {
		return err
	}

	//
	//  Authentication
	//

	var auth bytes.Buffer
	if _, err := auth.WriteString(g.options.auth.user); err != nil {
		g.disconnect(conn)
		return fmt.Errorf("failed to write username in authentication: %w", err)
	}
	if _, err := auth.WriteString(":"); err != nil {
		g.disconnect(conn)
		return fmt.Errorf("failed to write separator in authentication: %w", err)
	}
	password := sha1.Sum([]byte(g.options.auth.password))
	if _, err := auth.Write(password[:]); err != nil {
		g.disconnect(conn)
		return fmt.Errorf("failed to write password in authentication: %w", err)
	}
	if err := g.writeInitRequest(conn, []byte(hex.EncodeToString(auth.Bytes()))); err != nil {
		g.disconnect(conn)
		return fmt.Errorf("failed to authenticate: %w", err)
	}

	//
	// Registration
	//

	if err := g.writeInitRequest(conn, []byte("REGISTER UUID="+g.options.uuid+", TYPE=JSON")); err != nil {
		g.disconnect(conn)
		return fmt.Errorf("failed to register: %w", err)
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
		return fmt.Errorf("failed to request data stream: %w", err)
	}

	var stream stream
	stream.conn = conn
	stream.readTimeout = g.options.timeouts.read
	stream.timeRef = g.options.timeouts.timeRef
	stream.bufferSize = g.options.bufferSize

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
				events, err := stream.scan()
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
						g.options.logger.Printf("failed to read from dml event: %s", err)
					}
					continue
				}

				for i := range events {
					g.events <- events[i]
				}
			}
		}
	}()

	return nil
}

// Process starts consuming events, trigerring eventFunc callback for each
// event. This can be ran by multiple goroutines concurrently to speed-up the
// event processing.
func (g *Consumer) Process(eventFunc func(CDCEvent)) {
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

	buffer           bytes.Buffer
	bufferIdentation int
	bufferSize       int
}

func (s *stream) scan() ([]CDCEvent, error) {
	var responses []bytes.Buffer
	var loops int

	for {
		if err := s.conn.SetReadDeadline(s.timeRef().Add(s.readTimeout)); err != nil {
			return nil, fmt.Errorf("failed to set read timeout: %w", err)
		}

		buffer := make([]byte, s.bufferSize)
		n, err := s.conn.Read(buffer)
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}
		buffer = buffer[:n]

		var nonJSON bool
		for _, b := range buffer {
			var action bool
			if b == '{' {
				s.bufferIdentation++
				action = true
			} else if b == '}' {
				s.bufferIdentation--
				action = true
			}
			if s.bufferIdentation == 0 && s.buffer.Len() == 0 {
				// random test outside of the JSON object is a server error
				// message being returned
				nonJSON = true
			} else if s.bufferIdentation > 0 && nonJSON {
				// non-JSON data ended and new JSON data started
				var response bytes.Buffer
				response.Write(s.buffer.Bytes())

				if len(bytes.TrimSpace(response.Bytes())) > 0 {
					responses = append(responses, response)
				}

				s.buffer.Reset()
				nonJSON = false
			}
			if err := s.buffer.WriteByte(b); err != nil {
				return nil, fmt.Errorf("failed to process response byte: %w", err)
			}
			if action && s.bufferIdentation == 0 {
				var response bytes.Buffer
				response.Write(s.buffer.Bytes())

				if len(bytes.TrimSpace(response.Bytes())) > 0 {
					responses = append(responses, response)
				}

				s.buffer.Reset()
				action = false
			}
		}
		if nonJSON {
			// non-JSON data must fit inside a single buffer read call as we
			// can't determinate when it finishes
			var response bytes.Buffer
			response.Write(s.buffer.Bytes())

			if len(bytes.TrimSpace(response.Bytes())) > 0 {
				responses = append(responses, response)
			}

			s.buffer.Reset()
		}

		if len(responses) > 0 {
			break
		}

		loops++
		if loops > 100 {
			return nil, errors.New("too many network iterations to find a json object")
		}
	}

	var events []CDCEvent
	for i := range responses {
		if event, err := s.decodeEvent(responses[i].Bytes()); err == nil {
			events = append(events, event)
		} else {
			return nil, err
		}
	}
	return events, nil
}

func (s *stream) decodeEvent(response []byte) (CDCEvent, error) {
	switch {
	case reDDLEvent.Match(response):
		var ddlEvent DDLEvent
		if err := json.Unmarshal(response, &ddlEvent); err != nil {
			return nil, fmt.Errorf("failed to decode ddl event: %w", err)
		}
		return ddlEvent, nil

	case reDMLEvent.Match(response):
		var dmlEvent DMLEvent
		if err := json.Unmarshal(response, &dmlEvent); err != nil {
			return nil, fmt.Errorf("failed to decode dml event: %w", err)
		}
		dmlEvent.RawData = response
		return dmlEvent, nil
	}

	if err := checkResponseError(response); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("unknown maxscale event type: %s", string(response))
}

func checkResponseError(response []byte) error {
	if bytes.Contains(bytes.ToLower(response), []byte("err")) {
		return fmt.Errorf("error raised from maxscale: %s", string(response))
	}
	return nil
}

// CDCEventType is the type of the event.
type CDCEventType string

// List of possible event types.
const (
	// CDCEventTypeDDL DDL (Data Definition Language) are for database changes.
	CDCEventTypeDDL CDCEventType = "ddlEvent"
	// CDCEventTypeDML DML (Data Manipulation Language) are for data changes.
	CDCEventTypeDML CDCEventType = "dmlEvent"
)

// CDCEvent is the CDC event received from MaxScale.
type CDCEvent interface {
	EventType() CDCEventType
}

// DDLEvent is a MaxScale DDL event.
//
// https://github.com/mariadb-corporation/MaxScale/blob/6.2/Documentation/Routers/KafkaCDC.md#overview
type DDLEvent struct {
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
		RealType *string     `json:"real_type"`
		Length   *int        `json:"length"`
		Unsigned *bool       `json:"unsigned"`
	} `json:"fields"`
}

// EventType returns the type of the event.
func (d DDLEvent) EventType() CDCEventType {
	return CDCEventTypeDDL
}

// DMLEvent is a MaxScale DML event.
//
// https://github.com/mariadb-corporation/MaxScale/blob/6.2/Documentation/Routers/KafkaCDC.md#overview
type DMLEvent struct {
	Domain      int    `json:"domain"`
	ServerID    int    `json:"server_id"`
	Sequence    int    `json:"sequence"`
	EventNumber int    `json:"event_number"`
	Timestamp   int64  `json:"timestamp"`
	Type        string `json:"event_type"`
	RawData     []byte `json:"-"`
}

// EventType returns the type of the event.
func (d DMLEvent) EventType() CDCEventType {
	return CDCEventTypeDML
}
