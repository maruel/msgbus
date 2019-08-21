// Copyright 2016 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"errors"
	"sync"
)

// New returns a local thread safe memory backed Bus.
//
// This Bus is thread safe. It is useful for unit tests or as a local broker.
func New() Bus {
	return &local{persistentTopics: map[string][]byte{}}
}

type local struct {
	mu               sync.Mutex
	persistentTopics map[string][]byte
	subscribers      []*subscription
}

func (l *local) String() string {
	return "LocalBus"
}

// Close waits for all the internal goroutines to be done.
func (l *local) Close() error {
	l.mu.Lock()
	subs := l.subscribers
	l.persistentTopics = map[string][]byte{}
	l.subscribers = nil
	l.mu.Unlock()

	// Synchronously break all pending subscription.
	for _, s := range subs {
		s.closeSub()
	}
	return nil
}

func (l *local) Publish(msg Message, qos QOS) error {
	p, err := parseTopic(msg.Topic)
	if err != nil {
		return err
	}
	if p.isQuery() {
		return errors.New("cannot publish to a topic query")
	}
	subscribers := func() []*subscription {
		l.mu.Lock()
		defer l.mu.Unlock()
		// First handle retained message.
		if len(msg.Payload) == 0 {
			delete(l.persistentTopics, msg.Topic)
			return nil
		}
		if msg.Retained {
			b := make([]byte, len(msg.Payload))
			copy(b, msg.Payload)
			l.persistentTopics[msg.Topic] = b
		}

		// Now get the valid subscribers.
		var out []*subscription
		for i := range l.subscribers {
			if l.subscribers[i].topicQuery.match(msg.Topic) {
				out = append(out, l.subscribers[i])
			}
		}
		return out
	}()

	// Do the rest unlocked.
	if qos > BestEffort {
		// Synchronous.
		var wg sync.WaitGroup
		for i := range subscribers {
			wg.Add(1)
			subscribers[i].wg.Add(1)
			go func(s *subscription) {
				s.publish(msg)
				wg.Done()
			}(subscribers[i])
		}
		wg.Wait()
	} else {
		// Asynchronous.
		for i := range subscribers {
			subscribers[i].wg.Add(1)
			go subscribers[i].publish(msg)
		}
	}
	return nil
}

func (l *local) Subscribe(ctx context.Context, topicQuery string, qos QOS, c chan<- Message) error {
	// QOS is ignored.
	p, err := parseTopic(topicQuery)
	if err != nil {
		return err
	}

	// Subscribe and retrieve retained topics.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := ctx.Done()
	s := &subscription{topicQuery: p, c: c, done: done, cancel: cancel}
	msgs := func() []*Message {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.subscribers = append(l.subscribers, s)
		var out []*Message
		// If there is any retained topic matching, send them.
		for topic, payload := range l.persistentTopics {
			if p.match(topic) {
				out = append(out, &Message{Topic: topic, Payload: payload, Retained: true})
			}
		}
		return out
	}()

	// Synchronously send an empty message to signal subscription is completed.
	s.wg.Add(1)
	if !s.publish(Message{}) {
		return nil
	}

	// Synchronously send retained topics.
	for _, msg := range msgs {
		s.wg.Add(1)
		if !s.publish(*msg) {
			return nil
		}
	}

	// The streaming of items is done via Publish() implementation.
	<-done

	// Unsubscribe.
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, o := range l.subscribers {
		// Comparing pointers.
		if s == o {
			copy(l.subscribers[i:], l.subscribers[i+1:])
			l.subscribers = l.subscribers[:len(l.subscribers)-1]
			break
		}
	}
	// Compact array if necessary.
	if cap(l.subscribers) > 16 && cap(l.subscribers) >= 2*len(l.subscribers) {
		s := l.subscribers
		l.subscribers = make([]*subscription, len(s))
		copy(l.subscribers, s)
	}
	return nil
}

//

type subscription struct {
	topicQuery parsedTopic
	wg         sync.WaitGroup
	c          chan<- Message
	done       <-chan struct{}
	cancel     func()
}

// publish synchronously sends the message.
//
// s.wg.Add(1) must be called before.
func (s *subscription) publish(msg Message) bool {
	defer s.wg.Done()
	// Sadly we have to do a quick check first if s.done is set but s.c closed,
	// it may randomly crash with "panic: send on closed channel".
	// Remove this and the unit tests will randomly crash.
	select {
	case <-s.done:
		return false
	default:
	}

	select {
	case <-s.done:
		return false
	case s.c <- msg:
		return true
	}
}

// closeSub cancels the context, which will set the done channel, and wait for
// it to be done.
func (s *subscription) closeSub() {
	s.cancel()
	s.wg.Wait()
}

var _ Bus = &local{}
