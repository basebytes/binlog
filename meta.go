package binlog

import (
	"path/filepath"
	"slices"
	"sort"
	"sync"

	"github.com/basebytes/tools"
	"github.com/go-mysql-org/go-mysql/mysql"
)

const metaFile = "meta.json"

func newMetas() *Metas {
	return &Metas{metas: make(map[uint32]*Meta)}
}

type Metas struct {
	metas map[uint32]*Meta
	path  string
	lock  sync.RWMutex
}

func (m *Metas) Init(dir string) (err error) {
	m.path = filepath.Join(dir, metaFile)
	if tools.FileExists(m.path) {
		err = tools.DecodeFile(m.path, &m.metas)
	}
	if err == nil {
		for _, meta := range m.metas {
			for _, tables := range meta.Schema {
				for _, cols := range *tables {
					for _, col := range *cols {
						col.init()
					}
				}
			}
		}
	}
	return
}

func (m *Metas) GetMeta(key uint32) *Meta {
	_meta, ok := m.getMeta(key)
	if !ok {
		_meta = m.createMeta(key)
		m.Save()
	}
	return _meta
}

func (m *Metas) Save() {
	m.lock.Lock()
	defer m.lock.Unlock()
	_ = tools.EncodeObj(&m.metas, m.path)
}

func (m *Metas) getMeta(key uint32) (meta *Meta, ok bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	meta, ok = m.metas[key]
	return
}

func (m *Metas) createMeta(key uint32) *Meta {
	m.lock.Lock()
	defer m.lock.Unlock()
	meta := newMeta()
	m.metas[key] = meta
	return meta
}

func newMeta() *Meta {
	return &Meta{
		Position: &mysql.Position{},
		Schema:   make(map[string]*Table),
	}
}

type Meta struct {
	Position  *mysql.Position   `json:"position"`
	Schema    map[string]*Table `json:"schema"`
	Timestamp uint32            `json:"timestamp"`
}

func (m *Meta) GetOrNewColumns(schema, table string) (c *columns) {
	if tables, ok := m.Schema[schema]; !ok {
		tables = newTable()
		c = newColumns()
		tables.Set(table, c)
		m.Schema[schema] = tables
	} else if c, ok = tables.Get(table); !ok {
		c = newColumns()
		tables.Set(table, c)
	}
	return
}

func (m *Meta) GetColumns(schema, table string) (c *columns, ok bool) {
	if tables := m.Schema[schema]; tables != nil {
		c, ok = tables.Get(table)
	}
	return
}

func newTable() *Table {
	return &Table{}
}

type Table map[string]*columns

func (t *Table) Set(table string, columns *columns) {
	(*t)[table] = columns
}

func (t *Table) Get(table string) (c *columns, ok bool) {
	c, ok = (*t)[table]
	return
}

type column struct {
	Version uint32   `json:"version"`
	Names   []string `json:"names"`
	Types   []int    `json:"types"`
	pos     map[string]int
}

func (c *column) ColumnPos(names []string) (pos []int, found bool) {
	for _, name := range names {
		if p, ok := c.pos[name]; ok {
			pos = append(pos, p)
		} else {
			return
		}
	}
	found = true
	return
}

func (c *column) init() {
	if c.pos == nil {
		c.pos = make(map[string]int, len(c.Names))
	}
	for i, name := range c.Names {
		c.pos[name] = i
	}
}

func newColumns() *columns {
	return &columns{}
}

type columns []*column

func (c *columns) Add(version uint32, names []string, types []int) {
	col := &column{Version: version, Names: names, Types: types, pos: make(map[string]int, len(names))}
	col.init()
	*c = append(*c, col)
	sort.Sort(c)
}

func (c *columns) AppendIfNotEqualLast(version uint32, names []string, types []int) {
	if last := c.GetLast(); last == nil || !slices.Equal(names, last.Names) || !slices.Equal(types, last.Types) {
		if last == nil {
			version = 0
		}
		c.Add(version, names, types)
	}
}

func (c *columns) GetLast() (last *column) {
	if l := c.Len(); l > 0 {
		last = (*c)[l-1]
	}
	return
}

func (c *columns) GetLastColumn() (names []string) {
	if last := c.GetLast(); last != nil {
		names = last.Names
	}
	return
}

func (c *columns) GetLastColumnType() (types []int) {
	if last := c.GetLast(); last != nil {
		types = last.Types
	}
	return
}

func (c *columns) GetVersion(timestamp uint32) (version uint32) {
	for i := c.Len() - 1; i >= 0; i-- {
		version = c.getColumn(i).Version
		if timestamp > version {
			break
		}
	}
	return
}

func (c *columns) GetColumn(timestamp uint32) (col *column, ok bool) {
	for i := c.Len() - 1; i >= 0; i-- {
		col = c.getColumn(i)
		if ok = timestamp > col.Version; ok {
			break
		}
	}
	return
}

func (c *columns) getColumn(i int) *column {
	return (*c)[i]
}

func (c *columns) Len() int {
	return len(*c)
}

func (c *columns) Less(i, j int) bool {
	return (*c)[i].Version < (*c)[j].Version
}

func (c *columns) Swap(i, j int) {
	(*c)[i], (*c)[j] = (*c)[j], (*c)[i]
}
