package binlog

import (
	"errors"
	"sync"
)

var (
	consumer *Consumer
	lock     sync.RWMutex
	once     sync.Once
)

func Events(serverId uint32) (event <-chan *Event, ok bool) {
	if _consumer := getConsumer(); _consumer != nil {
		event, ok = _consumer.Event(serverId)
	}
	return
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
		lock.Lock()
		defer lock.Unlock()
		if _consumer, err := reload(dir, configs); err == nil {
			consumer = _consumer
			consumer.Start()
		} else {
			panic(err)
		}
	})
}

func Reload(dir string, configs []*Config) error {
	if getConsumer() == nil {
		return uninitializedErr
	}
	_consumer, err := reload(dir, configs)
	if err == nil {
		lock.Lock()
		defer lock.Unlock()
		if consumer != nil {
			consumer.Cancel()
		}
		consumer = _consumer
		consumer.Start()
	}
	return err
}

func reload(dir string, configs []*Config) (_consumer *Consumer, err error) {
	_consumer = newConsumer()
	err = _consumer.Init(dir, configs)
	return
}

func getConsumer() *Consumer {
	lock.RLock()
	defer lock.RUnlock()
	return consumer
}

var uninitializedErr = errors.New("binlog consumer uninitialized")
