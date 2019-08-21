// Copyright 2019 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"fmt"
	"log"
	"os"
)

func ExampleNew() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := New()
	c := make(chan Message)
	go func() {
		defer close(c)
		if err := b.Subscribe(ctx, "#", BestEffort, c); err != nil {
			log.Fatal(err)
		}
	}()
	// Wait for subscription to be live.
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		log.Fatal(msg)
	}

	if err := b.Publish(Message{Topic: "sensor", Payload: []byte("ON"), Retained: true}, BestEffort); err != nil {
		log.Fatal(err)
	}
	msg := <-c
	fmt.Printf("%s: %s\n", msg.Topic, msg.Payload)

	if err := b.Close(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// sensor: ON
}

func Example() {
	b := New()
	base := "homeassistant"
	var err error
	// Now all Publish() calls topics are based on "homeassistant/".
	if b, err = RebasePub(b, base); err != nil {
		log.Fatal(err)
	}
	// Now all Subscribe() calls topics are based on "homeassistant/".
	if b, err = RebaseSub(b, base); err != nil {
		log.Fatal(err)
	}

	if err := b.Close(); err != nil {
		log.Fatal(err)
	}
}

func ExampleNewMQTT() {
	will := Message{Topic: "alive", Payload: []byte("NO"), Retained: true}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	b, err := NewMQTT("tcp://localhost:1883", hostname, "user", "pass", will, false)
	if err != nil {
		log.Fatal(err)
	}
	msg := Message{Topic: "alive", Payload: []byte("YES"), Retained: true}
	if err := b.Publish(msg, BestEffort); err != nil {
		log.Fatal(err)
	}
	if err := b.Close(); err != nil {
		log.Fatal(err)
	}
}
