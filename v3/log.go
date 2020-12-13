// Copyright 2019 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"log"
	"sync"
)

// Log returns a Bus that logs all operations done on it, via log standard
// package.
func Log(b Bus) Bus {
	return &logging{Bus: b}
}

// Private code.

type logging struct {
	Bus
	wg sync.WaitGroup
}

// Close waits for all the internal goroutines to be done.
func (l *logging) Close() error {
	log.Printf("%s.Close()", l.Bus)
	err := l.Bus.Close()
	l.wg.Wait()
	return err
}

func (l *logging) Publish(msg Message, qos QOS) error {
	log.Printf("%s.Publish({%s, %q, %t}, %s)", l.Bus, msg.Topic, string(msg.Payload), msg.Retained, qos)
	return l.Bus.Publish(msg, qos)
}

func (l *logging) Subscribe(ctx context.Context, topicQuery string, qos QOS, c chan<- Message) error {
	log.Printf("%s.Subscribe(%s, %s)", l.Bus, topicQuery, qos)
	l.wg.Add(1)
	defer l.wg.Done()

	var err error
	c2 := make(chan Message)
	go func() {
		err = l.Bus.Subscribe(ctx, topicQuery, qos, c2)
		close(c2)
	}()
	done := ctx.Done()
loop:
	for msg := range c2 {
		log.Printf("%s <- Message{%s, %q}", l.Bus, msg.Topic, string(msg.Payload))
		// See comment in subscription.publish() for the rationale.
		select {
		case <-done:
			break loop
		default:
		}
		select {
		case <-done: // Hard to cover in unit test.
			break loop
		case c <- msg:
		}
	}
	// Drain to make sure the channel is closed, so that the goroutine is
	// completed.
	for range c2 {
	}
	return err
}
