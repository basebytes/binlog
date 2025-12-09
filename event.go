package binlog

func newEvent(db, schema, table, action string, data []map[string]any) *Event {
	return &Event{db: db, schema: schema, table: table, action: action, data: data}
}

type Event struct {
	db     string
	schema string
	table  string
	action string
	data   []map[string]any
}

func (e *Event) DB() string {
	return e.db
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

func (e *Event) Rows() []map[string]any {
	return e.data
}
