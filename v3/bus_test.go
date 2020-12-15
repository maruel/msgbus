// Copyright 2016 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQOS_String(t *testing.T) {
	t.Parallel()
	data := []struct {
		v        QOS
		expected string
	}{
		{BestEffort, "BestEffort"},
		{QOS(-1), "QOS(-1)"},
	}
	for _, line := range data {
		if actual := line.v.String(); actual != line.expected {
			t.Fatalf("%q != %q", actual, line.expected)
		}
	}
}

func TestRetained(t *testing.T) {
	t.Parallel()
	b := New()
	if err := b.Publish(Message{Topic: "bar", Payload: []byte("yo"), Retained: true}, BestEffort); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(Message{Topic: "baz", Payload: []byte("foo"), Retained: true}, BestEffort); err != nil {
		t.Fatal(err)
	}

	l, err := Retained(b, time.Millisecond, "bar", "baz")
	if err != nil {
		t.Fatal(l, err)
	}
	expected := map[string][]byte{"bar": []byte("yo"), "baz": []byte("foo")}
	if !reflect.DeepEqual(l, expected) {
		t.Fatalf("unexpected Retained: %v", l)
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetained_Err(t *testing.T) {
	t.Parallel()
	if _, err := Retained(nil, 0, ""); err == nil || err.Error() != "invalid topic \"\": empty topic" {
		t.Fatalf("expected invalid topic; got %v", err)
	}
	if _, err := Retained(nil, 0, "#"); err == nil || err.Error() != "cannot use topic query \"#\"" {
		t.Fatalf("expected can't use query; got %v", err)
	}
	if _, err := Retained(nil, 0, "a", "a"); err == nil || err.Error() != "cannot specify topic \"a\" twice" {
		t.Fatalf("expected can't use same topic twice; got %v", err)
	}
}

//

func TestParseTopicGood(t *testing.T) {
	t.Parallel()
	data := []string{
		"$",
		"/",
		"//",
		"+/+/+",
		"+/+/#",
		"sport/tennis/+",
		"sport/tennis/#",
		"sport/tennis/player1",
		"sport/tennis/player1/ranking",
		"sport/tennis/+/score/wimbledon",
		strings.Repeat("a", 65535),
	}
	for i, line := range data {
		if _, err := parseTopic(line); err != nil {
			t.Fatalf("%d: parseTopic(%#v) returned %v", i, line, err)
		}
	}
}

func TestParseTopicBad(t *testing.T) {
	t.Parallel()
	data := []string{
		"",
		"sport/tennis#",
		"sport/tennis/#/ranking",
		"sport/tennis+",
		"sport/#/tennis",
		strings.Repeat("a", 65536),
	}
	for i, line := range data {
		if _, err := parseTopic(line); err == nil {
			t.Fatalf("%d: parseTopic(%#v) returned non nil", i, line)
		}
	}
}

func TestMatchSuccess(t *testing.T) {
	t.Parallel()
	data := [][2]string{
		{"sport/tennis/#", "sport/tennis"},
		{"sport/tennis/#", "sport/tennis/player1"},
		{"sport/tennis/#", "sport/tennis/player1/ranking"},
		{"sport/tennis/+", "sport/tennis"},
		{"sport/tennis/+", "sport/tennis/player1"},
		{"sport/+/player1", "sport/tennis/player1"},
	}
	for i, line := range data {
		q, err := parseTopic(line[0])
		if err != nil {
			t.Fatalf("%d: %#v.match(%#v): %v", i, line[0], line[1], err)
		}
		if !q.match(line[1]) {
			t.Fatalf("%d: %#v.match(%#v) returned false", i, line[0], line[1])
		}
	}
}

func TestMatchFail(t *testing.T) {
	t.Parallel()
	data := [][2]string{
		{"sport/tennis/#", ""},
		{"sport/tennis/#", "sport"},
		{"sport/tennis/#", "sport/badminton"},
		{"sport/tennis/+", "sport"},
		{"sport/tennis/+", "sport/badminton"},
		{"sport/+/player1", "sport/tennis/player2"},
		{"sport/tennis", "sport/tennis/ball"},
		{"sport/tennis/ball", "sport/tennis"},
		{"sport/tennis/#", "sport/tennis/$player1"}, // 4.7.2
		{"+/tennis/", "sport/tennis/$player1"},      // 4.7.2
	}
	for i, line := range data {
		q, err := parseTopic(line[0])
		if err != nil {
			t.Fatalf("%d: %#v.match(%#v): %v", i, line[0], line[1], err)
		}
		if q.match(line[1]) {
			t.Fatalf("%d: %#v.match(%#v) returned true", i, line[0], line[1])
		}
	}
}
