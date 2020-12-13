// Copyright 2017 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestNew_Publish_ephemeral_sync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New()

	if err := b.Publish(Message{Topic: "foo", Payload: []byte("a")}, ExactlyOnce); err != nil {
		t.Fatal(err)
	}

	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "foo", ExactlyOnce, c); err != nil {
			t.Error(err)
		}
	}()
	// Wait for subscription to be live.
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		t.Fatal(msg)
	}

	go func() {
		if err := b.Publish(Message{Topic: "foo", Payload: []byte("b")}, ExactlyOnce); err != nil {
			t.Error(err)
		}
	}()
	if v := <-c; v.Topic != "foo" || string(v.Payload) != "b" {
		t.Fatalf("%s != foo; %q != b", v.Topic, string(v.Payload))
	}
	cancel()

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestNew_Publish_ephemeral_async(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New()

	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "foo", BestEffort, c); err != nil {
			t.Error(err)
		}
	}()
	// Wait for subscription to be live.
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		t.Fatal(msg)
	}

	if err := b.Publish(Message{Topic: "foo", Payload: make([]byte, 1)}, BestEffort); err != nil {
		t.Fatal(err)
	}
	if i := <-c; i.Topic != "foo" {
		t.Fatalf("%s != foo", i.Topic)
	}
	cancel()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestNew_Subscribe_Close(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New()

	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "foo", ExactlyOnce, c); err != nil {
			t.Error(err)
		}
	}()
	// Wait for subscription to be live.
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		t.Fatal(msg)
	}

	cancel()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestNew_Subscribe_blocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New()

	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "foo", ExactlyOnce, c); err != nil {
			t.Error(err)
		}
	}()

	// Here, we don't listen from c, instead we cancel the context.
	cancel()

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestNew_Subscribe_blocked_retained(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New()
	if err := b.Publish(Message{Topic: "foo", Payload: make([]byte, 1), Retained: true}, BestEffort); err != nil {
		t.Fatal(err)
	}

	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "foo", ExactlyOnce, c); err != nil {
			t.Error(err)
		}
	}()
	// Wait for subscription to be live.
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		t.Fatal(msg)
	}

	// The retained message cannot be sent.
	cancel()

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestNew_Subscribe_array_compaction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New()

	c := make(chan Message)
	go func() {
		if err := b.Subscribe(ctx, "foo", ExactlyOnce, c); err != nil {
			t.Error(err)
		}
	}()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Subscribe(ctx2, "foo", ExactlyOnce, c); err != nil {
				t.Error(err)
			}
		}()
	}

	// Wait for subscription to be live.
	for i := 0; i < 100; i++ {
		if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
			t.Fatal(msg)
		}
	}

	// The b.subscribers array will be compacted.
	cancel2()
	wg.Wait()
	cancel()

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNew_Publish_Retained(t *testing.T) {
	// Without subscription.
	b := New()
	if err := b.Publish(Message{Topic: "foo", Payload: []byte("yo"), Retained: true}, ExactlyOnce); err != nil {
		t.Fatal(err)
	}
	expected := map[string][]byte{"foo": []byte("yo")}
	if l, err := Retained(b, time.Millisecond, "foo"); err != nil || !reflect.DeepEqual(l, expected) {
		t.Fatal(l, err)
	}
	// Deleted retained message.
	if err := b.Publish(Message{Topic: "foo", Retained: true}, ExactlyOnce); err != nil {
		t.Fatal(err)
	}
	// Just enough time to "wait" but not enough to make this test too slow.
	if l, err := Retained(b, time.Millisecond, "foo"); err != nil || len(l) != 0 {
		t.Fatal(l, err)
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNew_Err(t *testing.T) {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		defer log.SetOutput(os.Stderr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New()

	if b.Publish(Message{Payload: []byte("yo")}, ExactlyOnce) == nil {
		t.Fatal("bad topic")
	}
	if b.Publish(Message{Topic: "#", Payload: make([]byte, 1)}, ExactlyOnce) == nil {
		t.Fatal("topic is query")
	}
	c := make(chan Message)
	if err := b.Subscribe(ctx, "", ExactlyOnce, c); err == nil {
		t.Fatal(err)
	}

	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "#", ExactlyOnce, c); err != nil {
			t.Error(err)
		}
	}()
	// Wait for subscription to be live.
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		t.Fatal(msg)
	}

	cancel()

	if l, err := Retained(b, time.Millisecond, ""); err == nil || len(l) != 0 {
		t.Fatal("bad topic")
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestNew_String(t *testing.T) {
	if s := New().(fmt.Stringer).String(); s != "LocalBus" {
		t.Fatal(s)
	}
}
