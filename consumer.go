package binlog

import (
	"context"
	"time"

	"github.com/basebytes/states"
)

type Consumer struct {
	ctx      context.Context
	cancel   context.CancelFunc
	state    *states.State
	metas    *Metas
	handlers map[uint32]*Handler
}

func newConsumer() *Consumer {
	return &Consumer{state: states.NewState(), metas: newMetas()}
}

func (c *Consumer) Init(dir string, configs []*Config) (err error) {
	if err = c.metas.Init(dir); err == nil {
		c.handlers = make(map[uint32]*Handler, len(configs))
		for _, cfg := range configs {
			if err = cfg.Init(); err != nil {
				break
			}
			handler := newHandler(c.metas.GetMeta(cfg.ServerID), c.metas.Save)
			if err = handler.Init(cfg); err != nil {
				break
			}
			c.handlers[cfg.ServerID] = handler
		}
	}
	return
}

func (c *Consumer) Start() {
	c.ctx, c.cancel = context.WithCancel(context.Background())
	for _, handler := range c.handlers {
		c.state.Add()
		handler.Start(c.ctx, c.state)
	}
}

func (c *Consumer) Stopped() bool {
	return c.state.Stopped()
}

func (c *Consumer) Cancel() {
	c.cancel()
	for !c.Stopped() {
		time.Sleep(time.Second)
	}
}

func (c *Consumer) Event(serverId uint32) (<-chan *Event, bool) {
	if !c.Stopped() {
		if handler, ok := c.handlers[serverId]; ok {
			return handler.Event(), true
		}
	}
	return nil, false
}
