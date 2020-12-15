// Copyright 2017 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestNewMQTT_fail(t *testing.T) {
	t.Parallel()
	_, err := NewMQTT("", "client", "user", "pass", Message{Topic: "status", Payload: []byte("dead"), Retained: true}, true)
	if err == nil {
		t.Fatal("invalid host")
	}
}

func TestNewMQTT_Publish_ephemeral_sync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newMQTT(&mqttState{}, newClientFake(t))
	if err != nil {
		t.Fatal(err)
	}

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

func TestNewMQTT_Publish_ephemeral_async(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newMQTT(&mqttState{}, newClientFake(t))
	if err != nil {
		t.Fatal(err)
	}

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

func TestNewMQTT_Subscribe_Close(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newMQTT(&mqttState{}, newClientFake(t))
	if err != nil {
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

	cancel()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-c; ok {
		t.Fatal("expected c to be closed")
	}
}

func TestNewMQTT_Subscribe_blocked(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newMQTT(&mqttState{}, newClientFake(t))
	if err != nil {
		t.Fatal(err)
	}

	cancel()

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	c := make(chan Message)
	if err := b.Subscribe(ctx, "foo", ExactlyOnce, c); err == nil || err.Error() != "client closed" {
		t.Fatal(err)
	}
}

func TestNewMQTT_Subscribe_blocked_retained(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newMQTT(&mqttState{}, newClientFake(t))
	if err != nil {
		t.Fatal(err)
	}

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

/*
func TestNewMQTT_Subscribe_array_compaction(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newMQTT(&mqttState{}, newClientFake(t))
	if err != nil {
		t.Fatal(err)
	}

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
				t.Fatal(err)
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
*/

func TestNewMQTT_Publish_Retained(t *testing.T) {
	t.Parallel()
	// Without subscription.
	b, err := newMQTT(&mqttState{}, newClientFake(t))
	if err != nil {
		t.Fatal(err)
	}
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

func TestNewMQTT_Err(t *testing.T) {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		defer log.SetOutput(os.Stderr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := newMQTT(&mqttState{}, newClientFake(t))
	if err != nil {
		t.Fatal(err)
	}

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
	if err := b.Publish(Message{Topic: "a"}, ExactlyOnce); err == nil || err.Error() != "client closed" {
		t.Fatal(err)
	}
}

func TestNewMQTT_String(t *testing.T) {
	t.Parallel()
	// Manually construct a fake connected instance to test methods.
	b, err := newMQTT(&mqttState{server: "foo"}, newClientFake(t))
	if err != nil {
		t.Fatal(err)
	}
	if s := b.(fmt.Stringer).String(); s != "MQTT{foo}" {
		t.Fatal(s)
	}
}

func TestMQTTState(t *testing.T) {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
		defer log.SetOutput(os.Stderr)
	}
	m := mqttState{}
	c := newClientFake(t)
	msg := &message{}
	m.unexpectedMessage(c, msg)
	m.onConnect(c)
	m.onConnectionLost(c, errors.New("oh"))
	m.onConnect(c)
}

//

// clientFake implements mqtt.Client but with a local msgbus implementation.
type clientFake struct {
	bus Bus
	t   *testing.T

	mu            sync.Mutex
	connected     bool
	subscriptions map[string]*subscription
}

func newClientFake(t *testing.T) *clientFake {
	return &clientFake{
		bus:           New(),
		t:             t,
		subscriptions: map[string]*subscription{},
	}
}

func (c *clientFake) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

func (c *clientFake) IsConnectionOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

func (c *clientFake) Connect() mqtt.Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connected {
		c.t.Error("already connected")
		return newTokenErr("already connected")
	}
	c.connected = true
	return newTokenDone()
}

func (c *clientFake) Disconnect(quiesce uint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.connected {
		c.t.Error("already disconnected")
	}
	c.connected = false
}

func (c *clientFake) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	t := newToken()
	go func() {
		t.err = c.bus.Publish(Message{Topic: topic, Payload: payload.([]byte), Retained: retained}, QOS(qos))
		close(t.done)
	}()
	return t
}

func (c *clientFake) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	ctx, cancel := context.WithCancel(context.Background())
	done := ctx.Done()
	ch := make(chan Message)
	s := &subscription{c: ch, done: done, cancel: cancel}
	c.mu.Lock()
	_, ok := c.subscriptions[topic]
	if !ok {
		c.subscriptions[topic] = s
	}
	c.mu.Unlock()
	if ok {
		return newTokenErr("topic already subscribed")
	}
	t := newTokenDone()
	s.wg.Add(1)
	go func() {
		t.err = c.bus.Subscribe(ctx, topic, QOS(qos), ch)
		s.wg.Done()
	}()
	<-ch
	if t.err != nil {
		// Failed.
		s.wg.Wait()
	} else {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				select {
				case <-done:
					return
				case msg, ok := <-ch:
					if !ok {
						return
					}
					callback(c, &message{msg})
				}
			}
		}()
	}
	return t
}

func (c *clientFake) SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token {
	//for topic, qos := range filters {
	//	c.Subscribe(topic, qos, callback)
	//}
	return newTokenErr("not implemented")
}

func (c *clientFake) Unsubscribe(topics ...string) mqtt.Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, topic := range topics {
		s, ok := c.subscriptions[topic]
		if !ok {
			return newTokenErr(fmt.Sprintf("topic %q not subscribed", topic))
		}
		s.cancel()
		delete(c.subscriptions, topic)
	}
	return newTokenDone()
}

func (c *clientFake) AddRoute(topic string, callback mqtt.MessageHandler) {
	c.t.Error("not implemented")
}

func (c *clientFake) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}

type token struct {
	mqtt.UnsubscribeToken // to get flowComplete()
	err                   error
	done                  chan struct{}
}

func (t *token) Wait() bool {
	<-t.done
	return t.err == nil
}
func (t *token) WaitTimeout(d time.Duration) bool {
	select {
	case <-t.done:
	case <-time.After(d):
	}
	return t.err == nil
}

func (t *token) Error() error {
	select {
	case <-t.done:
		return t.err
	default:
		return nil
	}
}

func newToken() *token {
	return &token{done: make(chan struct{})}
}

func newTokenDone() *token {
	t := newToken()
	close(t.done)
	return t
}

func newTokenErr(s string) *token {
	t := newTokenDone()
	t.err = errors.New(s)
	return t
}

type message struct {
	Message
}

func (m *message) Duplicate() bool {
	return false
}

func (m *message) Qos() byte {
	return 0
}

func (m *message) Retained() bool {
	return m.Message.Retained
}

func (m *message) Topic() string {
	return m.Message.Topic
}

func (m *message) MessageID() uint16 {
	return 0
}

func (m *message) Payload() []byte {
	return m.Message.Payload
}

func (m *message) Ack() {
}
