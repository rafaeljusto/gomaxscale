package gomaxscale

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

const initBufferSize = 1024

// Consumer is a Go implementation of a MaxScale CDC consumer.
type Consumer struct {
	address  string
	database string
	table    string
	options  Options
	stats    Stats

	events chan CDCEvent
	done   chan bool
}

// NewConsumer creates a new Consumer instance.
func NewConsumer(address, database, table string, optFuncs ...Option) *Consumer {
	opts := newDefaultOptions()
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
		if g.options.stats.period > 0 {
			statsTimer := time.NewTicker(g.options.stats.period)
			defer statsTimer.Stop()

			go func() {
				for {
					select {
					case <-statsTimer.C:
						g.options.stats.ticker(g.stats)
						g.stats.reset()
					case <-g.done:
						return
					}
				}
			}()
		}

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
	process := func(eventFunc func(CDCEvent), event CDCEvent) {
		defer func() {
			if r := recover(); r != nil {
				g.options.logger.Printf("panic detected while processing: %s", r)
			}
		}()

		if g.options.stats.period > 0 {
			start := time.Now()
			defer g.stats.add(time.Since(start))
		}

		eventFunc(event)
	}
	for event := range g.events {
		process(eventFunc, event)
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
