package binlog

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/basebytes/states"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

func newHandler(meta *Meta, save func()) *Handler {
	return &Handler{meta: meta, save: save}
}

type Handler struct {
	serverID  uint32
	ticker    *time.Ticker
	event     chan *Event
	ctx       context.Context
	cancel    context.CancelFunc
	canal     *canal.Canal
	meta      *Meta
	save      func()
	extractor *Extractor
}

func (c *Handler) Init(config *Config) (err error) {
	if c.canal, err = canal.NewCanal(c.canalConfig(config)); err == nil {
		if err = c.initPosition(); err == nil {
			if err = c.initTableColumns(config.Schemas); err == nil {
				c.serverID = config.ServerID
				c.ticker = time.NewTicker(config.SyncInterval)
				c.event = make(chan *Event, 100)
				c.extractor = newExtractor(c.meta, config.Schemas, c.getTableColumnInfo, c.save, c.event)
				c.canal.SetEventHandler(c.extractor)
			}
		} else {
			err = fmt.Errorf("addr[%s],get master position failed:%s", config.Addr(), err)
		}
	}
	return
}

func (c *Handler) Start(ctx context.Context, state *states.State) {
	c.ctx, c.cancel = context.WithCancel(ctx)
	go c.handle(state)
	go c.sync()
}

func (c *Handler) Event() <-chan *Event {
	return c.event
}

func (c *Handler) handle(state *states.State) {
	//从配置文件或者最后一个文件开始读取
	go func() {
		if err := c.canal.RunFrom(*c.meta.Position); err != nil {
			log.Printf("binlog handler[%d] consume binlog error:%s", c.serverID, err.Error())
		}
		close(c.event)
		state.Done()
	}()
	for {
		select {
		case <-c.ctx.Done():
			c.canal.Close()
			log.Printf("binlog handler[%d] receive cancel signal", c.serverID)
			return
		}
	}
}

func (c *Handler) getTableColumnInfo(db, table string) (names []string, types []int, err error) {
	if c.canal == nil || db == "" || table == "" {
		return
	}
	var sc *schema.Table
	if sc, err = c.canal.GetTable(db, table); err == nil {
		for _, _column := range sc.Columns {
			names = append(names, _column.Name)
			types = append(types, _column.Type)
		}
	}
	return
}

func (c *Handler) initTableColumns(schemas map[string]*Schema) error {
	for name, cfg := range schemas {
		for tableName := range cfg.Tables {
			cur, curType, err := c.getTableColumnInfo(name, tableName)
			if err != nil {
				err = fmt.Errorf("db[%s],get table[%s] schema failed:%s", name, tableName, err)
			} else if len(cur) == 0 {
				err = fmt.Errorf("db[%s],table[%s] schema not found", name, tableName)
			} else {
				ddd := c.meta.GetOrNewColumns(name, tableName)
				ddd.AppendIfNotEqualLast(uint32(time.Now().In(time.Local).Unix()), cur, curType)
				continue
			}
			return err
		}
	}
	return nil
}

func (c *Handler) initPosition() (err error) {
	if c.meta.Position.Name == "" {
		var pos mysql.Position
		if pos, err = c.canal.GetMasterPos(); err == nil {
			c.meta.Position.Name = pos.Name
		}
	}
	return
}

func (c *Handler) canalConfig(config *Config) *canal.Config {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Addr()
	cfg.User = config.UserName
	cfg.Password = config.Password
	cfg.ServerID = config.ServerID
	cfg.Logger = logger
	cfg.Dump = canal.DumpConfig{
		DiscardErr:     true,
		SkipMasterData: true,
	}
	return cfg
}

func (c *Handler) sync() {
	ctx, cancel := context.WithCancel(c.ctx)
	defer c.ticker.Stop()
	for {
		select {
		case <-c.ticker.C:
			c.save()
		case <-ctx.Done():
			cancel()
			return
		}
	}
}

type TableColumnInfo func(string, string) ([]string, []int, error)
