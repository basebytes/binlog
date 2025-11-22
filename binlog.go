package binlog

import "sync"

var (
	consumer *Consumer
	lock     sync.RWMutex
	once     sync.Once
)

func Events(serverId uint32) (<-chan *Event, bool) {
	lock.RLock()
	defer lock.RUnlock()
	if handler, ok := consumer.handlers[serverId]; ok && !consumer.Stopped() {
		return handler.event, ok
	}
	return nil, false
}

func Stopped() bool {
	lock.RLock()
	defer lock.RUnlock()
	return consumer.Stopped()
}

func Cancel() {
	lock.Lock()
	defer lock.Unlock()
	consumer.Cancel()
}

func Init(dir string, configs []*Config) {
	once.Do(func() {
		if err := Reload(dir, configs); err != nil {
			panic(err)
		}
	})
}

func Reload(dir string, configs []*Config) (err error) {
	_consumer := newConsumer()
	if err = _consumer.Init(dir, configs); err == nil {
		if consumer != nil {
			consumer.Cancel()
		}
		lock.Lock()
		lock.Unlock()
		consumer = _consumer
		consumer.Start()
	}
	return
}
