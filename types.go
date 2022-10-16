package gomaxscale

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

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
// https://github.com/mariadb-corporation/MaxScale/blob/maxscale-6.2.4/Documentation/Routers/KafkaCDC.md#overview
type DDLEvent struct {
	Namespace string          `json:"namespace"`
	Type      string          `json:"type"`
	Name      string          `json:"name"`
	Table     string          `json:"table"`
	Database  string          `json:"database"`
	Version   int             `json:"version"`
	GTID      string          `json:"gtid"`
	Fields    []DDLEventField `json:"fields"`
}

// DDLEventField is a field in a DDL event.
type DDLEventField struct {
	Name     string             `json:"name"`
	Type     DDLEventFieldValue `json:"type"`
	RealType *string            `json:"real_type"`
	Length   *int               `json:"length"`
	Unsigned *bool              `json:"unsigned"`
}

// UnmarshalJSON decodes the JSON into the field.
func (d *DDLEventField) UnmarshalJSON(b []byte) error {
	var tmp struct {
		Name     string      `json:"name"`
		Type     interface{} `json:"type"`
		RealType *string     `json:"real_type"`
		Length   *int        `json:"length"`
		Unsigned *bool       `json:"unsigned"`
	}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}

	d.Name = tmp.Name
	d.RealType = tmp.RealType
	d.Length = tmp.Length
	d.Unsigned = tmp.Unsigned

	switch t := tmp.Type.(type) {
	case string:
		d.Type = DDLEventFieldValueSimple{
			ValueType: t,
		}

	case []interface{}:
		var simple DDLEventFieldValueSimple
		for i := range t {
			value, ok := t[i].(string)
			if !ok {
				return fmt.Errorf("invalid simple value of type '%T'", t[i])
			}

			if strings.ToLower(value) == "null" {
				simple.Null = true
			}
			simple.ValueType = value
		}
		d.Type = simple

	case map[string]interface{}:
		typeStr, ok := t["type"].(string)
		if !ok {
			return fmt.Errorf("missing type in complex column definition: %#v", t)
		}
		switch strings.ToLower(typeStr) {
		case "enum":
			name, ok := t["name"].(string)
			if !ok {
				return fmt.Errorf("missing name in enum column definition: %#v", t)
			}
			symbols, ok := t["symbols"].([]interface{})
			if !ok {
				return fmt.Errorf("missing symbols in enum column definition: %#v", t)
			}

			var enum DDLEventFieldValueEnum
			enum.Name = name
			for i := range symbols {
				symbol, ok := symbols[i].(string)
				if !ok {
					return fmt.Errorf("symbol '%[1]v' (%[1]T) is not a string", symbols[i])
				}
				enum.Symbols = append(enum.Symbols, symbol)
			}

		default:
			return fmt.Errorf("unknown type '%s' in complex column definition: %#v", typeStr, t)
		}

	default:
		return fmt.Errorf("unknown type '%T' in column definition", t)
	}

	return nil
}

// EventType returns the type of the event.
func (d DDLEvent) EventType() CDCEventType {
	return CDCEventTypeDDL
}

// DDLEventFieldType is the type of a field in a DDL event.
type DDLEventFieldType string

// List of possible field types.
const (
	// DDLEventFieldTypeSimple is a simple field type.
	DDLEventFieldTypeSimple DDLEventFieldType = "simple"
	// DDLEventFieldTypeEnum is an enum field type.
	DDLEventFieldTypeEnum DDLEventFieldType = "enum"
)

// DDLEventFieldValue is the generic representation of a field in a DDL event.
type DDLEventFieldValue interface {
	Type() DDLEventFieldType
}

// DDLEventFieldValueSimple is a simple field type.
type DDLEventFieldValueSimple struct {
	ValueType string
	Null      bool
}

// Type returns the type of the field.
func (d DDLEventFieldValueSimple) Type() DDLEventFieldType {
	return DDLEventFieldTypeSimple
}

// DDLEventFieldValueEnum is an enum field type.
type DDLEventFieldValueEnum struct {
	Name    string
	Symbols []string
}

// Type returns the type of the field.
func (d DDLEventFieldValueEnum) Type() DDLEventFieldType {
	return DDLEventFieldTypeEnum
}

// DMLEvent is a MaxScale DML event.
//
// https://github.com/mariadb-corporation/MaxScale/blob/maxscale-6.2.4/Documentation/Routers/KafkaCDC.md#overview
type DMLEvent struct {
	Domain      int    `json:"domain"`
	ServerID    int    `json:"server_id"`
	Sequence    int    `json:"sequence"`
	EventNumber int    `json:"event_number"`
	Timestamp   int64  `json:"timestamp"`
	Type        string `json:"event_type"`

	// RawData contains all the JSON data related to this event. It should be
	// used to retrieve table specific data. For example, a table containing an
	// `id` and a `name` columns could be retrieved as the following:
	//
	//     var tableData struct {
	//       ID   int    `json:"id"`
	//       Name string `json:"name"`
	//     }
	//     err := json.Unmarshal(event.RawData, &tableData)
	//
	RawData []byte `json:"-"`
}

// EventType returns the type of the event.
func (d DMLEvent) EventType() CDCEventType {
	return CDCEventTypeDML
}

// Stats stores information about the running library in a specific period of
// time.
type Stats struct {
	NumberOfEvents int64
	ProcessingTime time.Duration
}

func (s *Stats) add(processingTime time.Duration) {
	atomic.AddInt64(&s.NumberOfEvents, 1)
	atomic.AddInt64((*int64)(&s.ProcessingTime), processingTime.Nanoseconds())
}

func (s *Stats) reset() {
	atomic.StoreInt64(&s.NumberOfEvents, 0)
	atomic.StoreInt64((*int64)(&s.ProcessingTime), 0)
}
