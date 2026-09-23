package main

import (
	"fmt"
	"sync"
	"time"
)

type Ops int

const (
	SET Ops = iota
	GET
	DELETE
	EXPIRE
	SHUTDOWN
)

type Message struct {
	Response string
	Err      error
}

type Event struct {
	Type     Ops
	Key      string
	Val      string
	Reply    chan Message
	TTL      uint32
	Callback func(msg Message)
}

type val struct {
	expiry time.Time
	data   string
}

type KeyValueStore struct {
	events     chan Event
	db         map[string]*val
	shutdownCh chan struct{}
	wg         *sync.WaitGroup
}

func NewKeyValueStore() *KeyValueStore {
	kv := KeyValueStore{
		events:     make(chan Event, 100),
		db:         make(map[string]*val),
		shutdownCh: make(chan struct{}),
	}

	wg := sync.WaitGroup{}
	kv.wg = &wg

	kv.wg.Add(1)
	go func(kv *KeyValueStore) {
		defer kv.wg.Done()
		kv.Run()
	}(&kv)

	return &kv
}

func (k *KeyValueStore) set(key string, value string) error {
	k.db[key] = &val{
		data: value,
	}
	return nil
}

func (k *KeyValueStore) get(key string) (string, error) {
	v, ok := k.db[key]
	if !ok {
		return "", fmt.Errorf("key: %s not found in db", key)
	}
	if v.expiry.After(time.Now()) {
		delete(k.db, key)
		return "", fmt.Errorf("key %s has reached expiry", key)
	}
	return v.data, nil
}

func (k *KeyValueStore) delete(key string) error {
	_, ok := k.db[key]
	if !ok {
		return fmt.Errorf("error in finding key %s in db", key)
	}
	delete(k.db, key)
	return nil
}

func (k *KeyValueStore) gc() error {
	now := time.Now()
	fmt.Println("starting garbage collector")
	for key, v := range k.db {
		if v.expiry.After(now) {
			fmt.Printf("garbage collecting key: %s\n", key)
			delete(k.db, key)
		}
	}
	return nil
}

func (k *KeyValueStore) setExpiry(key string, ttl time.Duration) error {
	v, ok := k.db[key]
	if !ok {
		return fmt.Errorf("key not found in db to set expiry")
	}

	v.expiry = time.Now().Add(ttl)
	return nil
}

func (k *KeyValueStore) Send(op Ops, inp ...any) error {
	switch op {
	case SET:
		if len(inp) != 3 {
			return fmt.Errorf("expecting 3 args (key, value, ttl) for SEND operation")
		}
		key, ok := inp[0].(string)
		if !ok {
			return fmt.Errorf("expecting key to be string for SET")
		}
		v, ok := inp[1].(string)
		if !ok {
			return fmt.Errorf("expecing v to string for SET")
		}
		ttl, ok := inp[2].(int)

		k.events <- Event{
			Type: op,
			Key:  key,
			Val:  v,
			TTL:  uint32(ttl),
		}

	}
	return nil
}

func (k *KeyValueStore) Run() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	defer k.wg.Done()

	for {
		select {
		case e := <-k.events:
			switch e.Type {
			case SET:
				if err := k.set(e.Key, e.Val); err != nil {
					// e.Reply <- Message{
					// 	Response: "",
					// 	Err:      fmt.Errorf("error SET = key: %s, val: %s, ttl: %v, err: %s", e.Key, e.Val, e.TTL, err.Error()),
					// }
					e.Callback(Message{
						Response: "",
						Err:      fmt.Errorf("error SET = key: %s, val: %s, ttl: %v, err: %s", e.Key, e.Val, e.TTL, err.Error()),
					})
				}
				e.Callback(Message{
					Response: fmt.Sprintf("SET success, key: %s", e.Key),
				})
				// e.Reply <- Message{
				// 	Response: fmt.Sprintf("SET success, key: %s", e.Key),
				// }

			case GET:
				v, err := k.get(e.Key)
				if err != nil {
					e.Reply <- Message{
						Response: "",
						Err:      fmt.Errorf("error GET = key: %s, err: %s", err.Error()),
					}
				}
				e.Reply <- Message{
					Response: fmt.Sprintf("GET success, key: %s, value: %s", e.Key, v),
				}

			case DELETE:
				err := k.delete(e.Key)
				if err != nil {
					e.Reply <- Message{
						Response: "",
						Err:      fmt.Errorf("error DELETE = key %s, err: %s", e.Key, err.Error()),
					}
				}
				e.Reply <- Message{
					Response: fmt.Sprintf("GET success, key: %s", e.Key),
				}

			case EXPIRE:
				err := k.setExpiry(e.Key, time.Duration(e.TTL))
				if err != nil {
					e.Reply <- Message{
						Err: fmt.Errorf("error EXPIRE = key: %s, err: %s", e.Key, err.Error()),
					}
				}
				e.Reply <- Message{
					Response: fmt.Sprintf("EXPIRE success, key: %s", e.Key),
				}

			case SHUTDOWN:
				fmt.Println("event loop closing")
				<-k.shutdownCh
				return
			}
		case <-t.C:
			k.gc()
		}
	}
}

func main() {
}
