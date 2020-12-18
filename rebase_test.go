// Copyright 2019 Marc-Antoine Ruel. All rights reserved.
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
	"testing"
	"time"
)

func TestRebasePub(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := RebasePub(New(), "foo")
	if err != nil {
		t.Fatal(err)
	}
	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "foo/bar", BestEffort, c); err != nil {
			t.Error(err)
		}
	}()
	// Wait for subscription to be live.
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		t.Fatal(msg)
	}

	if err := b.Publish(Message{Topic: "bar", Payload: []byte("yo"), Retained: true}, BestEffort); err != nil {
		t.Fatal(err)
	}
	if i := <-c; i.Topic != "foo/bar" {
		t.Fatalf("%s != foo/bar", i.Topic)
	}
	cancel()

	expected := map[string][]byte{"foo/bar": []byte("yo")}
	if l, err := Retained(b, 50*time.Millisecond, "foo/bar"); err != nil || !reflect.DeepEqual(l, expected) {
		t.Fatal(l, err)
	}
	if s := b.(fmt.Stringer).String(); s != "LocalBus/foo/" {
		t.Fatal(s)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestRebasePub_empty(t *testing.T) {
	t.Parallel()
	b := New()
	b2, err := RebasePub(b, "")
	if err != nil {
		t.Fatal(err)
	}
	if b != b2 {
		t.Fatal("expected exact same pointer")
	}
}

func TestRebasePub_Err(t *testing.T) {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		defer log.SetOutput(os.Stderr)
	}
	if b, err := RebasePub(New(), "a\000"); b != nil || err == nil {
		t.Fatal("bad topic")
	}
	if b, err := RebasePub(New(), "#"); b != nil || err == nil {
		t.Fatal("can't use a query")
	}
}

func TestRebasePub_Publish_Err(t *testing.T) {
	t.Parallel()
	b, err := RebasePub(New(), "foo")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(Message{Topic: "//bar", Payload: []byte("yo"), Retained: true}, BestEffort); err == nil {
		t.Fatal("invalid topic")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRebaseSub(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := RebaseSub(New(), "foo")
	if err != nil {
		t.Fatal(err)
	}
	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "bar", BestEffort, c); err != nil {
			t.Error(err)
		}
	}()
	// Wait for subscription to be live.
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		t.Fatal(msg)
	}

	if err := b.Publish(Message{Topic: "foo/bar", Payload: []byte("yo"), Retained: true}, BestEffort); err != nil {
		t.Fatal(err)
	}
	if msg := <-c; msg.Topic != "bar" {
		t.Fatalf("%s != bar", msg.Topic)
	}
	cancel()

	expected := map[string][]byte{"bar": []byte("yo")}
	if l, err := Retained(b, 50*time.Millisecond, "bar"); err != nil || !reflect.DeepEqual(l, expected) {
		t.Fatal(l, err)
	}
	if s := b.(fmt.Stringer).String(); s != "LocalBus/foo/" {
		t.Fatal(s)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestRebaseSub_empty(t *testing.T) {
	t.Parallel()
	b := New()
	b2, err := RebaseSub(b, "")
	if err != nil {
		t.Fatal(err)
	}
	if b != b2 {
		t.Fatal("expected exact same pointer")
	}
}

func TestRebaseSub_Subscribe_blocking(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := RebaseSub(New(), "foo")
	if err != nil {
		t.Fatal(err)
	}
	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "bar", BestEffort, c); err != nil {
			t.Error(err)
		}
	}()
	// Do not drain c, the channel will still be closed.
	cancel()

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestRebaseSub_Err(t *testing.T) {
	t.Parallel()
	if b, err := RebaseSub(New(), "a\000"); b != nil || err == nil {
		t.Fatal("bad topic")
	}
	if b, err := RebaseSub(New(), "#"); b != nil || err == nil {
		t.Fatal("can't use a query")
	}
}

func TestRebaseSub_Subscribe_Err(t *testing.T) {
	t.Parallel()
	b, err := RebaseSub(New(), "foo")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Subscribe(context.Background(), "#/a", BestEffort, make(chan Message)); err == nil {
		t.Fatal("bad topic")
	}
	if err := b.Subscribe(context.Background(), "//a", BestEffort, make(chan Message)); err == nil {
		t.Fatal("bad topic")
	}
}
