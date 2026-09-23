package main

import (
	"fmt"
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

type event struct {
	op  Ops
	key string
	val string
	ttl uint32
	fn  func(msg Message)
}

type EventInput struct {
	Key      string
	Val      string
	TTL      uint32
	Callback func(msg Message)
}

type val struct {
	expiry time.Time
	data   string
}

type KeyValueStore struct {
	events     chan event
	db         map[string]*val
	shutdownCh chan struct{}
}

func NewKeyValueStore() *KeyValueStore {
	kv := KeyValueStore{
		events:     make(chan event, 100),
		db:         make(map[string]*val),
		shutdownCh: make(chan struct{}),
	}

	go func(kv *KeyValueStore) {
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
	if !v.expiry.IsZero() && time.Now().After(v.expiry) {
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
	// fmt.Println("starting garbage collector")
	for key, v := range k.db {
		if !v.expiry.IsZero() && now.After(v.expiry) {
			// fmt.Printf("garbage collecting key: %s\n", key)
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

func (k *KeyValueStore) Send(op Ops, inp EventInput) error {
	ev := event{
		op:  op,
		key: inp.Key,
		fn:  inp.Callback,
	}
	switch op {
	case SET:
		ev.val = inp.Val
		k.events <- ev
	case GET:
		k.events <- ev
	case EXPIRE:
		ev.ttl = inp.TTL
		k.events <- ev
	case SHUTDOWN:
		k.events <- ev
	default:
		return fmt.Errorf("unsupported operation :%v", op)
	}

	return nil
}

func (k *KeyValueStore) Run() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()

	for {
		select {
		case e := <-k.events:
			switch e.op {
			case SET:
				if err := k.set(e.key, e.val); err != nil {
					e.fn(Message{
						Response: "",
						Err:      fmt.Errorf("error SET = key: %s, val: %s, ttl: %v, err: %s", e.key, e.val, e.ttl, err.Error()),
					})
					continue
				}
				e.fn(Message{
					Response: fmt.Sprintf("SET success, key: %s", e.key),
				})

			case GET:
				v, err := k.get(e.key)
				if err != nil {
					e.fn(Message{
						Response: "",
						Err:      fmt.Errorf("error GET = key: %s, err: %s", e.key, err.Error()),
					})
					continue
				}
				e.fn(Message{
					Response: v,
				})

			case DELETE:
				err := k.delete(e.key)
				if err != nil {
					e.fn(Message{
						Response: "",
						Err:      fmt.Errorf("error DELETE = key %s, err: %s", e.key, err.Error()),
					})
					continue
				}
				e.fn(Message{
					Response: fmt.Sprintf("GET success, key: %s", e.key),
				})

			case EXPIRE:
				err := k.setExpiry(e.key, time.Duration(int64(e.ttl)*int64(time.Second)))
				if err != nil {
					e.fn(Message{
						Err: fmt.Errorf("error EXPIRE = key: %s, err: %s", e.key, err.Error()),
					})
					continue
				}
				e.fn(Message{
					Response: fmt.Sprintf("EXPIRE success, key: %s", e.key),
				})

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
