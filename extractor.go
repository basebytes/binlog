package binlog

import (
	"log"
	"maps"
	"strings"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const InternalCol = "_UPDATES"

func newExtractor(meta *Meta, schemas map[string]*Schema, tableColumnInfo TableColumnInfo, save func(), out chan<- *Event) *Extractor {
	return &Extractor{meta: meta, schemas: schemas, tableColumnInfo: tableColumnInfo, save: save, out: out}
}

type Extractor struct {
	canal.DummyEventHandler
	tableColumnInfo TableColumnInfo
	schemas         map[string]*Schema
	meta            *Meta
	out             chan<- *Event
	stopped         atomic.Bool
	save            func()
}

func (h *Extractor) OnRotate(_ *replication.EventHeader, event *replication.RotateEvent) error {
	h.meta.Position = &mysql.Position{
		Name: string(event.NextLogName),
		Pos:  uint32(event.Position),
	}
	h.save()
	return nil
}

func (h *Extractor) OnTableChanged(header *replication.EventHeader, schema, table string) error {
	if _columns, ok := h.meta.GetColumns(schema, table); ok {
		if last := _columns.GetLast(); last != nil && header.Timestamp > last.Version {
			if names, types, err := h.tableColumnInfo(schema, table); err == nil {
				_columns.Add(header.Timestamp, names, types)
				h.save()
			}
		}
	}
	return nil
}

func (h *Extractor) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	if header == nil && force {
		h.meta.Position = &pos
		h.stopped.Store(true)
		h.save()
	}
	return nil
}

func (h *Extractor) OnRow(event *canal.RowsEvent) (err error) {
	defer func() {
		if !h.stopped.Load() {
			h.meta.Timestamp = event.Header.Timestamp
		}
	}()
	if colNames, colsPos, ok := h.check(event); ok {
		action := event.Action
		data := make([]map[string]any, 0, len(event.Rows))
		for i, row := range event.Rows {
			d := make(map[string]any)
			for j, p := range colsPos {
				d[colNames[j]] = row[p]
			}
			if action == canal.UpdateAction {
				if i&1 == 0 {
					if len(data) == 0 || data[len(data)-1] != nil {
						data = append(data, d)
					} else {
						data[len(data)-1] = d
					}
				} else if old := data[len(data)-1]; maps.Equal(old, d) {
					data[len(data)-1] = nil
				} else {
					updates := make([]string, 0, 3)
					for _, col := range colNames {
						if old[col] == d[col] {
							continue
						}
						updates = append(updates, col)
					}
					d[InternalCol] = updates
					data[len(data)-1] = d
				}
			} else {
				data = append(data, d)
			}
		}
		if data[len(data)-1] == nil {
			data = data[0 : len(data)-1]
		}
		schema := event.Table.Schema
		tableName := event.Table.Name
		if len(data) == 0 {
			log.Printf("schema[%s],table[%s],event[%s] has no data need to handler", schema, tableName, action)
			return
		}
		if !h.stopped.Load() {
			h.meta.Position.Pos = event.Header.LogPos
			h.out <- newEvent(schema, tableName, action, data)
		}
	}
	return
}

func (h *Extractor) check(event *canal.RowsEvent) (colNames []string, colsPos []int, ok bool) {
	if h.stopped.Load() && h.meta.Position.Pos <= event.Header.LogPos-event.Header.EventSize {
		return
	}
	eventTime := event.Header.Timestamp
	if h.meta.Timestamp > 0 && eventTime < h.meta.Timestamp {
		log.Printf("event time[%d] is less then sync progress[%d] skip", eventTime, h.meta.Timestamp)
		return
	}
	var (
		schema   = event.Table.Schema
		cfg      *Schema
		_columns *columns
		_column  *column
	)
	if cfg, ok = h.schemas[schema]; ok {
		tableName := event.Table.Name
		action := event.Action
		table := cfg.Tables[tableName]
		if ok = table != nil && table.ContainsKey(action) && len(event.Rows) > 0; ok {
			if ok = eventTime > cfg.FirstStart; !ok {
				log.Printf("eventTime[%d] is less then firstStart[%d] skip", eventTime, cfg.FirstStart)
			} else if ok = len(event.Rows[0]) == len(event.Table.Columns); !ok {
				log.Printf("event extractor data column length[%d] does not match the current table column length[%d] skip", len(event.Rows[0]), len(event.Table.Columns))
			} else if _columns, ok = h.meta.GetColumns(schema, tableName); !ok {
				log.Printf("schema[%s] table[%s] columns not found", schema, tableName)
			} else if _column, ok = _columns.GetColumn(eventTime); !ok {
				log.Printf("event time[%d]  is less than or equal to column version[%d] skip", eventTime, _column.Version)
			} else {
				colNames = table.Get(action)
				if colsPos, ok = _column.ColumnPos(colNames); !ok {
					log.Printf("required column [%s] not found, founded column [%s]", strings.Join(colNames, ","), strings.Join(_column.Names, ","))
				}
			}
		}
	}
	return
}
