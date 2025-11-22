package binlog

func newEvent(schema, table, action string, data []map[string]any) *Event {
	return &Event{schema: schema, table: table, action: action, data: data}
}

type Event struct {
	schema string
	table  string
	action string
	data   []map[string]any
}

func (e *Event) Schema() string {
	return e.schema
}

func (e *Event) Table() string {
	return e.table
}

func (e *Event) Action() string {
	return e.action
}

func (e *Event) Data() []map[string]any {
	return e.data
}
