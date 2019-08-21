// Copyright 2016 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"
)

// QOS defines the quality of service to use when publishing and subscribing to
// messages.
//
// The normative definition is
// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180912
type QOS int8

const (
	// BestEffort means the broker/client will deliver the message at most once,
	// with no confirmation.
	BestEffort QOS = 0
	// MinOnce means the broker/client will deliver the message at least once,
	// potentially duplicate.
	//
	// Do not use if message duplication is problematic.
	MinOnce QOS = 1
	// ExactlyOnce means the broker/client will deliver the message exactly once
	// by using a four step handshake.
	ExactlyOnce QOS = 2
)

const qosName = "BestEffortMinOnceExactlyOnce"

var qosIndex = [...]uint8{0, 10, 17, 28}

func (i QOS) String() string {
	if i < 0 || i >= QOS(len(qosIndex)-1) {
		return fmt.Sprintf("QOS(%d)", i)
	}
	return qosName[qosIndex[i]:qosIndex[i+1]]
}

// Message represents a single message to a single topic.
type Message struct {
	// Topic is the MQTT topic. It may have a prefix stripped by RebaseSub() or
	// inserted by RebasePub().
	Topic string
	// Payload is the application specific data.
	//
	// Publishing a message with no Payload deletes a retained Topic, and has no
	// effect on non-retained topic.
	Payload []byte
	// Retained signifies that the message is permanent until explicitly changed.
	// Otherwise it is ephemeral.
	Retained bool
}

// Bus is a publisher-subscriber bus.
//
// The topics are expected to use the MQTT definition. "Mosquitto" has good
// documentation about this: https://mosquitto.org/man/mqtt-7.html
//
// For more information about retained message behavior, see
// http://www.hivemq.com/blog/mqtt-essentials-part-8-retained-messages
//
// Implementation of Bus are expected to implement fmt.Stringer.
type Bus interface {
	io.Closer

	// Publish publishes a message to a topic.
	//
	// If msg.Payload is empty, the topic is deleted if it was retained.
	//
	// It is not guaranteed that messages are propagated in order, unless
	// qos ExactlyOnce is used.
	Publish(msg Message, qos QOS) error

	// Subscribe sends updates to this topic query through the provided channel.
	//
	// It blocks until the context is canceled. Returns an error if subscription
	// failed.
	//
	// Upon subscription, it sends an empty message that can be ignored, to
	// enable synchronizing with other systems.
	Subscribe(ctx context.Context, topicQuery string, qos QOS, c chan<- Message) error
}

// Retained retrieves all matching messages for one or multiple topics.
//
// Topic queries cannot be used.
//
// If a topic is missing, will wait for up to d for it to become available. If
// all topics are available, returns as soon as they are all retrieved.
func Retained(b Bus, d time.Duration, topic ...string) (map[string][]byte, error) {
	// Quick local check.
	var ps []parsedTopic
	for i, t := range topic {
		p, err := parseTopic(t)
		if err != nil {
			return nil, fmt.Errorf("invalid topic %q: %v", t, err)
		}
		if p.isQuery() {
			// TODO(maruel): Revisit this decision, this can be useful!
			return nil, fmt.Errorf("cannot use topic query %q", t)
		}
		for j := 0; j < i; j++ {
			if topic[j] == topic[i] {
				return nil, fmt.Errorf("cannot specify topic %q twice", t)
			}
		}
		ps = append(ps, p)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	c := make(chan Message)
	for _, t := range topic {
		t := t
		group.Go(func() error {
			return b.Subscribe(ctx, t, MinOnce, c)
		})
	}

	// Retrieve results.
	out := map[string][]byte{}
	done := ctx.Done()
	t := time.NewTimer(d)
loop:
	for {
		select {
		case msg := <-c:
			// Ignore the initial message.
			if len(msg.Topic) == 0 && len(msg.Payload) == 0 {
				continue
			}
			// Ignore non-retained message.
			if msg.Retained {
				out[msg.Topic] = msg.Payload
			}
			// Reset the timer after every message retrieved.
			if !t.Stop() {
				// This is impossible to deterministically add test coverage for this line.
				<-t.C
			}
		case <-t.C:
			cancel()
		case <-done:
			break loop
		}
		t.Reset(d)
	}

	cancel()

	// Empty any late message, this can happen in case of a subscription error,
	// which is unlikely.
loop2:
	for {
		select {
		// Hard to cover in unit test.
		case msg := <-c:
			if len(msg.Topic) == 0 && len(msg.Payload) == 0 {
				continue
			}
			// Ignore non-retained message, including the initial empty message.
			if msg.Retained {
				out[msg.Topic] = msg.Payload
			}
		default:
			break loop2
		}
	}
	return out, group.Wait()
}

// Private code.

// mergeTopic is used by RebasePub.
//
// root is guaranteed to not be empty and end with "/".
// topic is guaranteed to not be empty.
func mergeTopic(root, topic string) string {
	if len(root) == 0 || len(topic) == 0 || strings.HasPrefix(topic, "/") {
		return ""
	}
	return root + topic
}

// parsedTopic is either a query or a static topic.
type parsedTopic []string

func parseTopic(topic string) (parsedTopic, error) {
	if len(topic) == 0 {
		return nil, errors.New("empty topic")
	}
	if len(topic) > 65535 {
		return nil, fmt.Errorf("topic length %d over 65535 characters", len(topic))
	}
	if strings.ContainsRune(topic, rune(0)) || !utf8.ValidString(topic) {
		return nil, errors.New("topic must be valid UTF-8")
	}
	p := parsedTopic(strings.Split(topic, "/"))
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p parsedTopic) Validate() error {
	for i, e := range p {
		if i != len(p)-1 && e == "#" {
			return errors.New("wildcard # can only appear at the end of a topic query")
		} else if e != "+" && e != "#" {
			if strings.HasSuffix(e, "#") {
				return errors.New("wildcard # can not appear inside a topic section")
			}
			if strings.HasSuffix(e, "+") {
				return errors.New("wildcard + can not appear inside a topic section")
			}
		}
	}
	return nil
}

func (p parsedTopic) isQuery() bool {
	for _, e := range p {
		if e == "#" || e == "+" {
			return true
		}
	}
	return false
}

// match follows rules as defined at section 4.7:
// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html#_Toc442180919
func (p parsedTopic) match(topic string) bool {
	if len(topic) == 0 {
		return false
	}
	t := strings.Split(topic, "/")
	// 4.7.2
	isPrivate := strings.HasPrefix(t[len(t)-1], "$")
	for i, e := range p {
		if e == "#" {
			return !isPrivate
		}
		if e == "+" {
			if isPrivate {
				return false
			}
			if i == len(p)-1 && len(t) == len(p) || len(t) == len(p)-1 {
				return true
			}
			continue
		}
		if len(t) <= i || t[i] != e {
			return false
		}
	}
	return len(t) == len(p)
}
