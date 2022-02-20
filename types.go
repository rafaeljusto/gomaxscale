package gomaxscale

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
