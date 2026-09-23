package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func sendHelper(t *testing.T, kv *KeyValueStore, op Ops, inp EventInput) (string, error) {
	t.Helper()
	ch := make(chan Message, 1) // make it bounded so that the store's
	// go routines aren't blocked if no one is listening to the channel
	c := func(msg Message) {
		ch <- msg
	}
	inp.Callback = c

	err := kv.Send(op, inp)
	if err != nil {
		return "", err
	}

	select {
	case v, ok := <-ch:
		if !ok {
			return "", fmt.Errorf("unable to receive on channel")
		}
		if v.Err != nil {
			return "", v.Err
		}
		return v.Response, nil
	case <-time.After(time.Second):
		return "", fmt.Errorf("failed to recv event in time")
	}
}

func TestFunctionality(t *testing.T) {
	kv := NewKeyValueStore()

	// test SET
	v, err := sendHelper(t, kv, SET, EventInput{
		Key: "key1",
		Val: "val1",
	})
	if err != nil {
		t.Error(err)
	}

	// test SET 2
	v, err = sendHelper(t, kv, SET, EventInput{
		Key: "key2",
		Val: "val2",
	})
	if err != nil {
		t.Error(err)
	}

	// test GET
	v, err = sendHelper(t, kv, GET, EventInput{
		Key: "key1",
	})
	if err != nil {
		t.Error(err)
	}
	if v != "val1" {
		t.Error(fmt.Errorf("expected val1 but observed: %s", v))
	}

	// test EXPIRE for KEY1
	v, err = sendHelper(t, kv, EXPIRE, EventInput{
		Key: "key1",
		TTL: uint32(10),
	})
	if err != nil {
		t.Error(err)
	}

	// test GET before 10 second timeout for 'key1'
	v, err = sendHelper(t, kv, GET, EventInput{
		Key: "key1",
	})
	if err != nil {
		t.Error(err)
	}
	if v != "val1" {
		t.Errorf("expected val1 but observed: %s", v)
	}

	// test GET after 10 seconds for 'key1'
	time.Sleep(15 * time.Second)
	v, err = sendHelper(t, kv, GET, EventInput{
		Key: "key1",
	})
	if err == nil {
		t.Error(fmt.Errorf("expected error in GET of expired key: 'key1', but got val: %s", v))
	}
}

func TestChurn(t *testing.T) {
	kv := NewKeyValueStore()
	lim := 10000
	errs := make(chan error, lim)
	wg := sync.WaitGroup{}

	for i := range lim {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sendHelper(t, kv, SET, EventInput{
				Key: key,
				Val: val,
			})
			if err != nil {
				errs <- err
			}

			v, err := sendHelper(t, kv, GET, EventInput{
				Key: key,
			})
			if err != nil {
				errs <- err
			}
			if v != val {
				errs <- fmt.Errorf("observed: %s, expected: %s, key: %s", v, val, key)
			}
		}()
	}

	wg.Wait()

	errs_n := len(errs)
	if errs_n > 0 {
		errMsg := ""
		c := 0
		for err := range errs {
			errMsg += err.Error()
			if c > 10 {
				errMsg += "..........."
				break
			}
			errMsg += "\n"
			c++
		}
		t.Errorf("errs = %v", errMsg)
	}
}

func TestRace(t *testing.T) {
	kv := NewKeyValueStore()
	lim := 10000
	errs := make(chan error, lim)
	wg := sync.WaitGroup{}

	for i := range lim {
		key := "key1"
		val := fmt.Sprintf("val-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()

			if i%2 == 0 {
				time.Sleep(5 * time.Millisecond)
			}

			if i == lim-1 {
				fmt.Println("waiting for 15 seconds to ensure this is latest update")
				time.Sleep(15 * time.Second)
			}

			_, err := sendHelper(t, kv, SET, EventInput{
				Key: key,
				Val: val,
			})
			if err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()

	errs_n := len(errs)
	if errs_n > 0 {
		errMsg := ""
		c := 0
		for err := range errs {
			errMsg += err.Error()
			if c > 10 {
				errMsg += "..........."
				break
			}
			errMsg += "\n"
			c++
		}
		t.Errorf("errs = %v", errMsg)
	}

	v, err := sendHelper(t, kv, GET, EventInput{
		Key: "key1",
	})
	if err != nil {
		t.Error(err)
	}
	expected := fmt.Sprintf("val-%d", lim-1)
	if v != expected {
		t.Errorf("expected: %s, observed: %s", expected, v)
	}
}
