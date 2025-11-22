package binlog

import (
	"fmt"
	"slices"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
)

const (
	defaultMySqlPort    = 3306
	defaultSyncInterval = time.Minute
)

type Config struct {
	ServerID     uint32             `json:"serverId,omitempty"`
	Host         string             `json:"host,omitempty"`
	UserName     string             `json:"userName,omitempty"`
	Password     string             `json:"password,omitempty"`
	Port         int                `json:"port,omitempty"`
	SyncInterval time.Duration      `json:"syncInterval,omitempty"`
	Schemas      map[string]*Schema `json:"schemas,omitempty"`
}

func (c *Config) Init() (err error) {
	if c.Port <= 0 {
		c.Port = defaultMySqlPort
	}
	if c.SyncInterval <= 0 {
		c.SyncInterval = defaultSyncInterval
	}
	if c.ServerID <= 0 {
		err = fmt.Errorf("binlog config serverId not found")
	} else if c.UserName == "" {
		err = fmt.Errorf("binlog config userName not found")
	} else {
		for name, schema := range c.Schemas {
			switch {
			case schema.DB == "":
				err = fmt.Errorf("schema config db not found")
			case schema.FirstStart < 0:
				err = fmt.Errorf("invalid schema config firstStart")
			case len(schema.Tables) == 0:
				err = fmt.Errorf("binlog  schema config table not found")
			default:
				schema.Name = name
				for _, actions := range schema.Tables {
					if actions.Size() == 0 {
						actions.Set(canal.InsertAction, []string{})
						actions.Set(canal.UpdateAction, []string{})
						actions.Set(canal.DeleteAction, []string{})
					} else {
						for action, cols := range *actions {
							if action != canal.InsertAction && action != canal.UpdateAction && action != canal.DeleteAction {
								err = fmt.Errorf("invalid binlog config schema table action %s", action)
							} else {
								slices.Sort(cols)
							}
						}
					}
				}
			}
			if err != nil {
				break
			}
		}
	}
	return
}

func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

type Schema struct {
	DB         string
	Name       string
	FirstStart uint32 //unix seconds
	Tables     map[string]*Action
}

func (db *Schema) GetColumns(table, action string) (columns []string) {
	if actions := db.Tables[table]; actions != nil {
		columns = actions.Get(action)
	}
	return
}

type Action map[string][]string

func (a *Action) Size() int {
	return len(*a)
}

func (a *Action) Set(action string, columns []string) {
	(*a)[action] = columns
	if len(columns) > 0 {
		slices.Sort((*a)[action])
	}
}

func (a *Action) Appends(action string, columns ...string) {
	added := make([]string, 0, len(columns))
	for _, _column := range columns {
		if slices.Contains((*a)[action], _column) {
			continue
		}
		added = append(added, _column)
	}
	if len(added) > 0 {
		(*a)[action] = append((*a)[action], added...)
		slices.Sort((*a)[action])
	}
}

func (a *Action) ContainsKey(action string) (ok bool) {
	_, ok = (*a)[action]
	return
}

func (a *Action) Contains(action string, column string) (ok bool) {
	switch action {
	case canal.InsertAction, canal.UpdateAction, canal.DeleteAction:
		ok = slices.Contains(a.Get(action), column)
	}
	return
}

func (a *Action) Get(action string) []string {
	return (*a)[action]
}
