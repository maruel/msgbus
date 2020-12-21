// Copyright 2019 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestMQTT_Integration(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "linux" {
		t.Skipf("skipping on %s", runtime.GOOS)
	}
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	if len(run(t, "docker", "images", "-q", image)) == 0 {
		run(t, "docker", "pull", "-q", image)
	} else {
		t.Logf("found docker image %s", image)
	}

	tmpDir, err := ioutil.TempDir("", "mqtt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err2 := os.RemoveAll(tmpDir); err2 != nil {
			t.Fatal(err2)
		}
	}()
	t.Logf("using path %s", tmpDir)
	if err = os.MkdirAll(filepath.Join(tmpDir, "config"), 0755); err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile(filepath.Join(tmpDir, "config", "mosquitto.conf"), []byte(mosquittoConfig), 0644); err != nil {
		t.Fatal(err)
	}

	p := getFreePort(t)
	t.Logf("using port %d", p)
	dockerid := strings.TrimSpace(run(t, "docker", "run", "-p", fmt.Sprintf("127.0.0.1:%d:1883", p), "-v", fmt.Sprintf("%s:/mosquitto", tmpDir), "--detach", image))
	t.Logf("docker id %s", dockerid)
	defer run(t, "docker", "rm", "-f", dockerid)

	addr := fmt.Sprintf("127.0.0.1:%d", p)
	if l, err2 := net.Listen("tcp", addr); l != nil {
		l.Close()
		t.Log("port check success")
	} else if err2 == nil {
		t.Fatal("Expected port to be bound")
	}

	t.Log("connecting")
	bus, err := NewMQTT("tcp://"+addr, "test", "", "", Message{}, true)
	if err != nil {
		t.Fatal(err)
	}

	// Will run:
	// - Subscribe
	// - Publish
	// - Retrieve
	// - Stop server
	// - Start
	// - Publish
	// - Retrieve

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan Message)
	go func() {
		defer close(c)
		t.Log("subscribing")
		if err := bus.Subscribe(ctx, "foo", ExactlyOnce, c); err != nil {
			t.Errorf("Subscribe: %s", err)
		}
		t.Log("subscription done")
	}()
	t.Log("Wait for subscription to be live")
	if msg := <-c; len(msg.Topic) != 0 || len(msg.Payload) != 0 || msg.Retained {
		t.Fatal(msg)
	}

	t.Log("Publishing")
	if err := bus.Publish(Message{Topic: "foo", Payload: make([]byte, 1)}, BestEffort); err != nil {
		t.Fatal(err)
	}
	t.Log("Reading")
	if i := <-c; i.Topic != "foo" {
		t.Fatalf("%s != foo", i.Topic)
	}

	run(t, "docker", "stop", "-t", "60", dockerid)

	run(t, "docker", "start", dockerid)
	t.Log("Publishing")
	if err := bus.Publish(Message{Topic: "foo", Payload: make([]byte, 1)}, ExactlyOnce); err != nil {
		t.Fatal(err)
	}

	t.Log("Reading")
	if i := <-c; i.Topic != "foo" {
		t.Fatalf("%s != foo", i.Topic)
	}

	t.Log("Closing")
	cancel()
	<-c
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
}

const image = "eclipse-mosquitto:1.6.7"

const mosquittoConfig = `
persistence true
persistence_location /mosquitto/data/

# Auth?
#password_file /run/secrets/mosquitto_passwd

## Ports

listener 1883
protocol mqtt
`

func getFreePort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}

func run(t *testing.T, args ...string) string {
	t.Logf("Running: %s", strings.Join(args, " "))
	cmd := exec.Command(args[0], args[1:]...)
	b := bytes.Buffer{}
	cmd.Stdout = &b
	cmd.Stderr = &b
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to run %v\n%s", args, &b)
	}
	return b.String()
}

/*
func init() {
	mqtt.CRITICAL = log.New(os.Stderr, "MQTT CRITICAL: ", log.Lmicroseconds)
	mqtt.ERROR = log.New(os.Stderr, "MQTT ERROR: ", log.Lmicroseconds)
	mqtt.WARN = log.New(os.Stderr, "MQTT WARN:  ", log.Lmicroseconds)
	mqtt.DEBUG = log.New(os.Stderr, "MQTT DEBUG: ", log.Lmicroseconds)
}
*/
