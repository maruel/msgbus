// Copyright 2019 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"io"
	"log"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	if !testing.Verbose() {
		log.SetOutput(io.Discard)
		defer log.SetOutput(os.Stderr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := Log(New())
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

	// Works asynchronously with BestEffort.
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

func TestLog_Subscribe(t *testing.T) {
	if !testing.Verbose() {
		log.SetOutput(io.Discard)
		defer log.SetOutput(os.Stderr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := Log(New())
	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "#", BestEffort, c); err != nil {
			t.Error(err)
		}
	}()
	// Wait for subscription to be active.
	<-c
	if err := b.Publish(Message{Topic: "bar", Payload: []byte("yo")}, ExactlyOnce); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}
