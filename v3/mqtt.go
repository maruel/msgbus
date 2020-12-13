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
	ms := &mqttState{server: server}
	opts.OnConnect = ms.onConnect
	opts.OnConnectionLost = ms.onConnectionLost
	opts.DefaultPublishHandler = ms.unexpectedMessage
	return newMQTT(ms, mqtt.NewClient(opts))
}

func newMQTT(ms *mqttState, c mqtt.Client) (Bus, error) {
	m := &mqttBus{client: c, ms: ms}
	token := m.client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return nil, err
	}
	return m, nil
}

//

type mqttState struct {
	server           string
	mu               sync.Mutex
	disconnectedOnce bool
}

func (m *mqttState) String() string {
	return fmt.Sprintf("MQTT{%s}", m.server)
}

func (m *mqttState) unexpectedMessage(c mqtt.Client, msg mqtt.Message) {
	log.Printf("%s: Unexpected message %s", m, msg.Topic())
}

func (m *mqttState) onConnect(c mqtt.Client) {
	m.mu.Lock()
	d := m.disconnectedOnce
	m.mu.Unlock()
	if d {
		log.Printf("%s: connected", m)
	}
}

func (m *mqttState) onConnectionLost(c mqtt.Client, err error) {
	log.Printf("%s: connection lost: %v", m, err)
	m.mu.Lock()
	m.disconnectedOnce = true
	m.mu.Unlock()
}

// mqttBus main purpose is to hide the complex thing that paho.mqtt.golang is.
//
// This Bus is thread safe.
type mqttBus struct {
	client mqtt.Client
	wg     sync.WaitGroup
	ms     *mqttState
}

func (m *mqttBus) String() string {
	return m.ms.String()
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
	token := m.client.Subscribe(topicQuery, byte(qos), func(client mqtt.Client, msg mqtt.Message) {
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
		return err
	}

	// Signal that subscription is complete.
	select {
	case <-done:
	case c <- Message{}:
	}
	<-done

	token2 := m.client.Unsubscribe(topicQuery)
	token2.Wait()
	return token2.Error()
}

var _ Bus = &mqttBus{}
