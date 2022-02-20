package gomaxscale

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"time"
)

var (
	reDDLEvent = regexp.MustCompile(`{"namespace":`)
	reDMLEvent = regexp.MustCompile(`{"domain":`)
)

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
