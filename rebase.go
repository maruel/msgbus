// Copyright 2019 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// RebasePub rebases a Bus when publishing messages.
//
// All Message published have their Topic prefixed with root.
//
// Messages retrieved are unaffected.
//
// Returns nil if root is an invalid topic or if it is a topic query.
func RebasePub(b Bus, root string) (Bus, error) {
	if len(root) == 0 {
		return b, nil
	}
	if root[len(root)-1] != '/' {
		root += "/"
	}
	t := root[:len(root)-1]
	p, err := parseTopic(t)
	if err != nil {
		return nil, err
	}
	if p.isQuery() {
		return nil, errors.New("cannot use topic query")
	}
	return &rebasePublisher{b, root}, nil
}

// RebaseSub rebases a Bus when subscribing or getting topics.
//
// All Message retrieved have their Topic prefix root stripped.
//
// Messages published are unaffected.
//
// Returns nil if root is an invalid topic or if it is a topic query.
func RebaseSub(b Bus, root string) (Bus, error) {
	if len(root) == 0 {
		return b, nil
	}
	if root[len(root)-1] != '/' {
		root += "/"
	}
	t := root[:len(root)-1]
	p, err := parseTopic(t)
	if err != nil {
		return nil, err
	}
	if p.isQuery() {
		return nil, errors.New("cannot use topic query")
	}
	return &rebaseSubscriber{Bus: b, root: root}, nil
}

// Private code.

type rebasePublisher struct {
	Bus
	root string
}

func (r *rebasePublisher) String() string {
	return fmt.Sprintf("%s/%s", r.Bus, r.root)
}

func (r *rebasePublisher) Publish(msg Message, qos QOS) error {
	msg.Topic = mergeTopic(r.root, msg.Topic)
	if len(msg.Topic) == 0 {
		return errors.New("invalid topic")
	}
	return r.Bus.Publish(msg, qos)
}

type rebaseSubscriber struct {
	Bus
	root string
	wg   sync.WaitGroup
}

func (r *rebaseSubscriber) String() string {
	return fmt.Sprintf("%s/%s", r.Bus, r.root)
}

func (r *rebaseSubscriber) Subscribe(ctx context.Context, topicQuery string, qos QOS, c chan<- Message) error {
	topicQuery = mergeTopic(r.root, topicQuery)
	if len(topicQuery) == 0 {
		return errors.New("invalid topic")
	}
	var err error
	c2 := make(chan Message)
	r.wg.Add(2)
	go func() {
		err = r.Bus.Subscribe(ctx, topicQuery, qos, c2)
		close(c2)
		r.wg.Done()
	}()

	done := ctx.Done()
	offset := len(r.root)
	// Translate the topics.
loop:
	for msg := range c2 {
		if !(len(msg.Topic) == 0 && len(msg.Payload) == 0) {
			msg.Topic = msg.Topic[offset:]
		}
		// See comment in subscription.publish() for the rationale.
		select {
		// Hard to cover in unit test, c2 is likely to be closed before done.
		case <-done:
			break loop
		default:
		}
		select {
		case <-done: // Hard to cover in unit test.
		case c <- msg:
		}
	}
	r.wg.Done()
	return err
}
