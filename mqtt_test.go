// Copyright 2017 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestNewMQTT_fail(t *testing.T) {
	_, err := NewMQTT("", "client", "user", "pass", Message{Topic: "status", Payload: []byte("dead"), Retained: true}, true)
	if err == nil {
		t.Fatal("invalid host")
	}
}

func TestMQTT(t *testing.T) {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		defer log.SetOutput(os.Stderr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Manually construct a fake connected instance to test methods.
	cf := clientFake{}
	m := &mqttBus{client: &cf, server: "localhost"}

	if s := m.String(); s != "MQTT{localhost}" {
		t.Fatal(s)
	}
	if err := m.Publish(Message{Topic: "a", Payload: []byte("b")}, ExactlyOnce); err != nil {
		t.Fatal(err)
	}

	c := make(chan Message)
	go func() {
		defer close(c)
		if err := m.Subscribe(ctx, "#", BestEffort, c); err != nil {
			t.Fatal(err)
		}
	}()
	// Wait for subscription to be live.
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		t.Fatal(msg)
	}

	cancel()
	if l, err := Retained(m, time.Second, "a/#/b"); err == nil || len(l) != 0 {
		t.Fatal("bad topic")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestMQTT_Err(t *testing.T) {
	cf := clientFake{}
	m := &mqttBus{client: &cf, server: "localhost"}

	if err := m.Publish(Message{Topic: "", Payload: []byte("b")}, BestEffort); err == nil || err.Error() != "empty topic" {
		t.Fatalf("invalid: %v", err)
	}
	if err := m.Publish(Message{Topic: "#", Payload: []byte("b")}, BestEffort); err == nil || err.Error() != "cannot publish to a topic query" {
		t.Fatalf("invalid: %v", err)
	}
	if err := m.Subscribe(context.Background(), "", BestEffort, make(chan Message)); err == nil || err.Error() != "empty topic" {
		t.Fatalf("invalid: %v", err)
	}
}

//

// clientFake implements mqtt.Client.
type clientFake struct {
	err error
}

func (c *clientFake) IsConnected() bool {
	return true
}

func (c *clientFake) IsConnectionOpen() bool {
	return true
}

func (c *clientFake) Connect() mqtt.Token {
	return &tokenFake{err: c.err}
}

func (c *clientFake) Disconnect(quiesce uint) {
}

func (c *clientFake) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	return &tokenFake{err: c.err}
}

func (c *clientFake) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	return &tokenFake{err: c.err}
}

func (c *clientFake) SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token {
	return &tokenFake{err: c.err}
}

func (c *clientFake) Unsubscribe(topics ...string) mqtt.Token {
	return &tokenFake{err: c.err}
}

func (c *clientFake) AddRoute(topic string, callback mqtt.MessageHandler) {
}

func (c *clientFake) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}

type tokenFake struct {
	mqtt.UnsubscribeToken // to get flowComplete()
	err                   error
}

func (t *tokenFake) Wait() bool {
	return true
}
func (t *tokenFake) WaitTimeout(time.Duration) bool {
	return true
}

func (t *tokenFake) Error() error {
	return t.err
}
