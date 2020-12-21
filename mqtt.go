// Copyright 2016 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// NewMQTT returns an initialized active MQTT connection.
//
// The connection timeouts are fine tuned for a LAN. It will likely fail on a
// slower connection or when used over the internet.
//
// will is the message to send if the connection is not closed correctly; when
// Close() is not called.
//
// order determines is messages are processed in order or not. Out of order
// processing means that a subscription will not be blocked by another one that
// fails to process its queue in time.
//
// This main purpose of this library is to create a layer that is simpler, more
// usable and more Go-idiomatic than paho.mqtt.golang.
//
// See
// https://godoc.org/github.com/eclipse/paho.mqtt.golang#ClientOptions.AddBroker
// for the accepted server format.
func NewMQTT(server, clientID, user, password string, will Message, order bool) (Bus, error) {
	return newMQTT(server, clientID, user, password, will, order, mqtt.NewClient)
}

func newMQTT(server, clientID, user, password string, will Message, order bool, f func(*mqtt.ClientOptions) mqtt.Client) (*mqttBus, error) {
	opts := mqtt.NewClientOptions().AddBroker(server)
	opts.ClientID = clientID
	// Default 10min is too slow.
	opts.MaxReconnectInterval = 30 * time.Second
	opts.Order = order
	opts.Username = user
	opts.Password = password
	if len(will.Topic) != 0 {
		opts.SetBinaryWill(will.Topic, will.Payload, byte(ExactlyOnce), true)
	}

	m := &mqttBus{server: server}
	opts.OnConnect = m.onConnect
	opts.OnConnectionLost = m.onConnectionLost
	opts.DefaultPublishHandler = m.unexpectedMessage
	m.client = f(opts)
	token := m.client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return nil, err
	}
	return m, nil
}

// mqttBus main purpose is to hide the complex thing that paho.mqtt.golang is.
//
// This Bus is thread safe.
type mqttBus struct {
	server string
	client mqtt.Client

	wg sync.WaitGroup

	mu               sync.Mutex
	disconnectedOnce bool
}

func (m *mqttBus) String() string {
	return fmt.Sprintf("MQTT{%s}", m.server)
}

// Close gracefully closes the connection to the server.
//
// Waits 1s for the connection to terminate correctly. If this function is not
// called, the will message in NewMQTT() will be activated.
func (m *mqttBus) Close() error {
	m.client.Disconnect(1000)
	m.wg.Wait()
	m.client = nil
	return nil
}

func (m *mqttBus) Publish(msg Message, qos QOS) error {
	if m.client == nil {
		return errors.New("client closed")
	}
	// Quick local check.
	p, err := parseTopic(msg.Topic)
	if err != nil {
		return err
	}
	if p.isQuery() {
		return errors.New("cannot publish to a topic query")
	}
	m.wg.Add(1)
	token := m.client.Publish(msg.Topic, byte(qos), msg.Retained, msg.Payload)
	if qos > BestEffort {
		token.Wait()
	}
	m.wg.Done()
	return token.Error()
}

// Subscribe implements Bus.
//
// One big difference with New() implementation is that subscribing twice to
// the same topic will fail.
func (m *mqttBus) Subscribe(ctx context.Context, topicQuery string, qos QOS, c chan<- Message) error {
	if m.client == nil {
		return errors.New("client closed")
	}
	// Quick local check.
	if _, err := parseTopic(topicQuery); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := ctx.Done()
	m.wg.Add(1)
	defer m.wg.Done()
	// This is cheezy but there's a race condition where the subscription
	// function can be called after subscription is canceled.
	mu := sync.RWMutex{}
	// Block the subscription callback from sending anything on the channel until
	// the signal message below is sent.
	mu.Lock()
	token := m.client.Subscribe(topicQuery, byte(qos), func(client mqtt.Client, msg mqtt.Message) {
		mu.RLock()
		defer mu.RUnlock()
		select {
		case <-done:
			return
		default:
		}
		select {
		case <-done:
			// Context was canceled, skip on sending message.
		case c <- Message{Topic: msg.Topic(), Payload: msg.Payload(), Retained: msg.Retained()}:
		}
	})
	token.Wait()
	if err := token.Error(); err != nil {
		mu.Unlock()
		// We assume our subscribe function will never have been called.
		return err
	}

	// Signal that subscription is complete.
	select {
	case <-done:
	case c <- Message{}:
	}
	// It's now safe for the subscription function to send messages.
	mu.Unlock()

	// Wait for the context to be canceled.
	<-done

	token2 := m.client.Unsubscribe(topicQuery)
	token2.Wait()
	// Sadly, there could still be subscribe function about to be called. We need
	// to make sure they are not leaking.
	mu.Lock()
	err := token2.Error()
	mu.Unlock()
	return err
}

func (m *mqttBus) unexpectedMessage(c mqtt.Client, msg mqtt.Message) {
	log.Printf("%s: Unexpected message %s", m, msg.Topic())
}

func (m *mqttBus) onConnect(c mqtt.Client) {
	m.mu.Lock()
	d := m.disconnectedOnce
	m.mu.Unlock()
	if d {
		log.Printf("%s: connected", m)
	}
}

func (m *mqttBus) onConnectionLost(c mqtt.Client, err error) {
	log.Printf("%s: connection lost: %v", m, err)
	m.mu.Lock()
	m.disconnectedOnce = true
	m.mu.Unlock()
}

var _ Bus = &mqttBus{}
