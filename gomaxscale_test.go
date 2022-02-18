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

	"github.com/rafaeljusto/gomaxscale"
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
			var events int
			server.events = func() *gomaxscale.Event {
				if events++; events > 5 {
					return nil
				}
				return &gomaxscale.Event{
					Domain:      1,
					ServerID:    1,
					Sequence:    1,
					EventNumber: events,
					Timestamp:   time.Now().Unix(),
					Type:        "insert",
				}
			}
		},
	}, {
		name: "it should fail to authenticate",
		config: func(server *maxScaleServerMock) {
			server.failAuthentication = true
			server.events = func() *gomaxscale.Event {
				return nil
			}
		},
		wantErr: "failed to authenticate",
	}, {
		name: "it should fail to register",
		config: func(server *maxScaleServerMock) {
			server.failRegistration = true
			server.events = func() *gomaxscale.Event {
				return nil
			}
		},
		wantErr: "failed to register",
	}}

	var server maxScaleServerMock
	serverAddr, err := server.start(t)
	if err != nil {
		t.Fatalf("failed to start server: %s", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer server.reset()
			tt.config(&server)

			consumer := gomaxscale.NewConsumer(serverAddr, "database", "table")
			_, err := consumer.Start()
			switch {
			case err != nil && tt.wantErr == "":
				t.Errorf("unexpected error starting consumer: %s", err)
			case err == nil && tt.wantErr != "":
				t.Error("expected error starting consumer not raised")
			case err != nil && !strings.Contains(err.Error(), tt.wantErr):
				t.Errorf("unexpected error starting consumer: %s", err)
			}
			consumer.Close()
		})
	}
}

func ExampleConsumer_Start() {
	consumer := gomaxscale.NewConsumer("127.0.0.1:4001", "database", "table",
		gomaxscale.WithAuth("maxuser", "maxpwd"),
	)
	dataStream, err := consumer.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	fmt.Printf("started consuming events from database '%s' table '%s'\n",
		dataStream.Database, dataStream.Table)

	done := make(chan bool)
	go func() {
		consumer.Process(func(event gomaxscale.Event) {
			fmt.Printf("event '%s' detected\n", event.Type)
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
	server := maxScaleServerMock{
		events: func() *gomaxscale.Event {
			return nil
		},
	}

	serverAddr, err := server.start(b)
	if err != nil {
		b.Fatalf("failed to start server: %s", err)
	}
	defer server.close(b)

	consumer := gomaxscale.NewConsumer(serverAddr, "database", "table")
	if _, err = consumer.Start(); err != nil {
		b.Fatalf("failed to start consumer: %s", err)
	}

	for n := 0; n < b.N; n++ {
		consumer.Process(func(event gomaxscale.Event) {})
	}
}

type logger interface {
	Logf(format string, args ...interface{})
}

type maxScaleServerMock struct {
	failAuthentication bool
	failRegistration   bool
	initialEvent       gomaxscale.DataStream
	events             func() *gomaxscale.Event

	listener net.Listener
	wgConns  sync.WaitGroup

	done     chan bool
	finished chan bool
}

func (m *maxScaleServerMock) start(l logger) (string, error) {
	m.done = make(chan bool)
	m.finished = make(chan bool)

	var err error
	m.listener, err = net.Listen("tcp", ":0")
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

	//
	// Initial event
	//

	encoder := json.NewEncoder(conn)

	if err := encoder.Encode(m.initialEvent); err != nil {
		_, _ = writeString(conn, "ERR failed encoding initial event: %s", err)
		return
	}

	//
	// Events
	//

	for {
		select {
		case <-m.done:
			return
		default:
		}

		event := m.events()
		if event == nil {
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
