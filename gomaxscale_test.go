package gomaxscale_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rafaeljusto/gomaxscale/v2"
)

const initBufferSize = 1024

var (
	reAuth         = regexp.MustCompile(`^.*?:.*?$`)
	reRegistration = regexp.MustCompile(`^REGISTER UUID=.+?, TYPE=JSON$`)
	reDataStream   = regexp.MustCompile(`^REQUEST-DATA .+?\..+?(?:.+?)?(?: .+?)?$`)
)

func TestCustomer_Start(t *testing.T) {
	tests := []struct {
		name    string
		config  func(*maxScaleServerMock)
		wantErr string
	}{{
		name: "it should start a consumer correctly",
		config: func(server *maxScaleServerMock) {
			server.events = func() gomaxscale.CDCEvent {
				return nil
			}
		},
	}, {
		name: "it should fail to authenticate",
		config: func(server *maxScaleServerMock) {
			server.failAuthentication = true
		},
		wantErr: "failed to authenticate",
	}, {
		name: "it should fail to register",
		config: func(server *maxScaleServerMock) {
			server.failRegistration = true
		},
		wantErr: "failed to register",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var server maxScaleServerMock
			serverAddr, err := server.start(t)
			if err != nil {
				t.Fatalf("failed to start server: %s", err)
			}
			defer server.close(t)

			tt.config(&server)

			consumer := gomaxscale.NewConsumer(serverAddr, "database", "table")
			err = consumer.Start()
			switch {
			case err != nil && tt.wantErr == "":
				t.Errorf("unexpected error: %s", err)
			case err == nil && tt.wantErr != "":
				t.Error("expected error not raised")
			case err != nil && !strings.Contains(err.Error(), tt.wantErr):
				t.Errorf("unexpected error: %s", err)
			}
			consumer.Close()
		})
	}
}

func TestCustomer_Process(t *testing.T) {
	tests := []struct {
		name      string
		config    func(*maxScaleServerMock)
		want      func(gomaxscale.CDCEvent) error
		wantCount int
		wantLogs  string
	}{{
		name: "it should process some events correctly",
		config: func(server *maxScaleServerMock) {
			var eventsIndex int
			server.events = func() gomaxscale.CDCEvent {
				eventsIndex++
				switch eventsIndex {
				case 1:
					return gomaxscale.DDLEvent{
						Database: "database",
						Table:    "table",
					}
				case 2:
					return gomaxscale.DMLEvent{
						Type: "insert",
					}
				}
				return nil
			}
		},
		want: func() func(event gomaxscale.CDCEvent) error {
			var eventsIndex int
			return func(event gomaxscale.CDCEvent) error {
				eventsIndex++
				switch eventsIndex {
				case 1:
					ddlEvent, ok := event.(gomaxscale.DDLEvent)
					if !ok {
						return fmt.Errorf("expected ddl event instead of %T", event)
					}
					expected := gomaxscale.DDLEvent{
						Database: "database",
						Table:    "table",
					}
					if ddlEvent.Database != expected.Database ||
						ddlEvent.Table != expected.Table {
						return fmt.Errorf("unexpected ddl event: %+v", ddlEvent)
					}

				case 2:
					dmlEvent, ok := event.(gomaxscale.DMLEvent)
					if !ok {
						return fmt.Errorf("expected dml event instead of %T", event)
					}
					expected := gomaxscale.DMLEvent{
						Type: "insert",
					}
					if dmlEvent.Type != expected.Type {
						return fmt.Errorf("unexpected dml event: %+v", dmlEvent)
					}

				default:
					return fmt.Errorf("unexpected event: %+v", event)
				}

				return nil
			}
		}(),
		wantCount: 2,
	}, {
		name: "it should fail when retrieving events",
		config: func(server *maxScaleServerMock) {
			server.failEvents = true
			server.events = func() gomaxscale.CDCEvent {
				return nil
			}
		},
		wantLogs: "events failed",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var server maxScaleServerMock
			serverAddr, err := server.start(t)
			if err != nil {
				t.Fatalf("failed to start server: %s", err)
			}
			defer server.close(t)

			tt.config(&server)

			var logMsg string
			loggerMock := loggerMock{
				mockPrint: func(v ...interface{}) {
					logMsg = fmt.Sprint(v...)
				},
				mockPrintf: func(format string, v ...interface{}) {
					logMsg = fmt.Sprintf(format, v...)
				},
			}

			consumer := gomaxscale.NewConsumer(serverAddr, "database", "table",
				gomaxscale.WithLogger(loggerMock),
			)
			if err := consumer.Start(); err != nil {
				t.Fatalf("failed to start consumer: %s", err)
			}

			var count int
			done := make(chan bool)
			go func() {
				consumer.Process(func(event gomaxscale.CDCEvent) {
					if event == nil {
						consumer.Close()
						done <- true
					}
					count++
					if err := tt.want(event); err != nil {
						t.Errorf("failed to process event %d: %s", count, err)
					}
					if count == tt.wantCount || err != nil {
						consumer.Close()
						done <- true
					}
				})
			}()

			// safety check to prevent server running forever
			timer := time.NewTicker(100 * time.Millisecond)
			defer timer.Stop()

			select {
			case <-done:
			case <-timer.C:
				t.Log("timeout waiting for consumer to finish")
			}

			if count != tt.wantCount {
				t.Errorf("unexpected event count: %d", count)
			}
			if !strings.Contains(logMsg, tt.wantLogs) {
				t.Errorf("unexpected logs: %s", logMsg)
			}
		})
	}
}

func ExampleConsumer_Process() {
	consumer := gomaxscale.NewConsumer("127.0.0.1:4001", "database", "table",
		gomaxscale.WithAuth("maxuser", "maxpwd"),
	)
	err := consumer.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	fmt.Println("start consuming events")

	done := make(chan bool)
	go func() {
		consumer.Process(func(event gomaxscale.CDCEvent) {
			switch e := event.(type) {
			case gomaxscale.DDLEvent:
				fmt.Printf("ddl event detected on database '%s' and table '%s'\n",
					e.Database, e.Table)
			case gomaxscale.DMLEvent:
				fmt.Printf("dml '%s' event detected\n", e.Type)
			}
		})
		done <- true
	}()

	signalChanel := make(chan os.Signal, 1)
	signal.Notify(signalChanel, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signalChanel:
	case <-done:
	}

	fmt.Println("terminating")
}

func BenchmarkConsumer_Process(b *testing.B) {
	b.StopTimer()

	server := maxScaleServerMock{
		events: func() gomaxscale.CDCEvent {
			return gomaxscale.DMLEvent{
				Type: "insert",
			}
		},
	}

	serverAddr, err := server.start(b)
	if err != nil {
		b.Fatalf("failed to start server: %s", err)
	}
	defer server.close(b)

	loggerMock := loggerMock{
		mockPrint: func(v ...interface{}) {
			b.Log(v...)
		},
		mockPrintf: func(format string, v ...interface{}) {
			b.Logf(format, v...)
		},
	}

	consumer := gomaxscale.NewConsumer(serverAddr, "database", "table",
		gomaxscale.WithTimeout(10*time.Millisecond, 10*time.Millisecond),
		gomaxscale.WithLogger(loggerMock),
	)
	if err = consumer.Start(); err != nil {
		b.Fatalf("failed to start consumer: %s", err)
	}
	defer consumer.Close()

	b.StartTimer()
	consumer.Process(func(event gomaxscale.CDCEvent) {})
}

type logger interface {
	Logf(format string, args ...interface{})
}

type maxScaleServerMock struct {
	failAuthentication bool
	failRegistration   bool
	failEvents         bool

	events func() gomaxscale.CDCEvent

	listener net.Listener
	wgConns  sync.WaitGroup

	done     chan bool
	finished chan bool
}

func (m *maxScaleServerMock) start(l logger) (string, error) {
	m.done = make(chan bool)
	m.finished = make(chan bool)

	var err error
	m.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("could not listen on a random port: %s", err)
	}

	go func() {
		defer func() {
			m.wgConns.Wait()
			m.finished <- true
		}()

	listenConns:
		for {
			conn, err := m.listener.Accept()
			if err != nil {
				select {
				case <-m.done:
					break listenConns
				default:
				}

				l.Logf("failed to accept connection: %s", err)
				continue
			}

			go m.handleConnection(conn)
		}
	}()

	return m.listener.Addr().String(), nil
}

func (m *maxScaleServerMock) close(l logger) {
	defer m.reset()
	if m.done == nil {
		return
	}
	close(m.done)
	if err := m.listener.Close(); err != nil {
		l.Logf("failed to shutdown server: %s", err)
	}
	<-m.finished
}

func (m *maxScaleServerMock) reset() {
	m.failAuthentication = false
	m.failRegistration = false
	m.failEvents = false
	m.events = nil
}

func (m *maxScaleServerMock) handleConnection(conn net.Conn) {
	m.wgConns.Add(1)
	defer m.wgConns.Done()

	//
	//  Authentication
	//

	authRequest := make([]byte, initBufferSize)
	n, err := read(conn, authRequest)
	if err != nil {
		_, _ = writeString(conn, "ERR failed to read authentication request: %s", err)
		return
	}
	authRequest = authRequest[:n]

	authRequestDecoded, err := hex.DecodeString(string(authRequest))
	if err != nil {
		_, _ = writeString(conn, "ERR failed to decode authentication request: %s", err)
		return
	}

	if !reAuth.Match(authRequestDecoded) {
		_, _ = writeString(conn, "ERR invalid authentication format")
		return
	}

	if m.failAuthentication {
		_, _ = writeString(conn, "ERR authentication failed")
		return
	}

	_, _ = writeString(conn, "OK")

	//
	//  Registration
	//

	registrationRequest := make([]byte, initBufferSize)
	n, err = read(conn, registrationRequest)
	if err != nil {
		_, _ = writeString(conn, "ERR failed to read registration request: %s", err)
		return
	}
	registrationRequest = registrationRequest[:n]

	if !reRegistration.Match(registrationRequest) {
		_, _ = writeString(conn, "ERR invalid registration format")
		return
	}

	if m.failRegistration {
		_, _ = writeString(conn, "ERR registration failed")
		return
	}

	_, _ = writeString(conn, "OK")

	//
	// Data stream
	//

	dataStreamRequest := make([]byte, initBufferSize)
	n, err = read(conn, dataStreamRequest)
	if err != nil {
		_, _ = writeString(conn, "ERR failed to read data stream request: %s", err)
		return
	}
	dataStreamRequest = dataStreamRequest[:n]

	if !reDataStream.Match(dataStreamRequest) {
		_, _ = writeString(conn, "ERR invalid data stream format")
		return
	}

	if m.failEvents {
		_, _ = writeString(conn, "ERR events failed")
		return
	}

	//
	// Events
	//

	encoder := json.NewEncoder(conn)

	for {
		select {
		case <-m.done:
			return
		default:
		}

		event := m.events()
		if event == nil {
			// tests finished
			break
		}
		if err := encoder.Encode(event); err != nil {
			_, _ = writeString(conn, "ERR failed encoding event: %s", err)
			return
		}
	}
}

func read(conn net.Conn, b []byte) (int, error) {
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return 0, fmt.Errorf("failed to set read deadline: %s", err)
	}
	n, err := conn.Read(b)
	if err != nil {
		return 0, fmt.Errorf("failed to read data: %s", err)
	}
	return n, nil
}

func writeString(conn net.Conn, format string, a ...interface{}) (int, error) {
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		return 0, fmt.Errorf("failed to set write deadline: %s", err)
	}
	n, err := fmt.Fprintf(conn, format, a...)
	if err != nil {
		return 0, fmt.Errorf("failed to write data: %s", err)
	}
	return n, nil
}

type loggerMock struct {
	mockPrint  func(v ...interface{})
	mockPrintf func(format string, v ...interface{})
}

func (l loggerMock) Print(v ...interface{}) {
	l.mockPrint(v...)
}

func (l loggerMock) Printf(format string, v ...interface{}) {
	l.mockPrintf(format, v...)
}
