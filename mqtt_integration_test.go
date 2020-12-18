// Copyright 2019 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package msgbus

import (
	"bytes"
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

	c := exec.Command("docker", "images", "-q", image)
	b := bytes.Buffer{}
	c.Stdout = &b
	if err := c.Run(); err != nil {
		t.Fatal("docker is not accessible")
	}
	if b.Len() == 0 {
		b.Reset()
		c = exec.Command("docker", "pull", "-q", image)
		c.Stdout = &b
		c.Stderr = &b
		if err := c.Run(); err != nil {
			t.Fatalf("failed to pull %s: %s\n%s", image, err, b.String())
		}
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
	args := []string{"run", "-p", fmt.Sprintf("127.0.0.1:%d:1883", p), "-v", fmt.Sprintf("%s:/mosquitto", tmpDir), "--detach", image}
	t.Logf("docker %s", strings.Join(args, " "))
	c = exec.Command("docker", args...)
	b.Reset()
	c.Stdout = &b
	c.Stderr = &b
	if err = c.Run(); err != nil {
		t.Fatalf("failed to start %s: %s\nLog: %s", c.Args, err, b.String())
	}
	dockerid := strings.TrimSpace(b.String())
	t.Logf("docker id %s", dockerid)
	defer func() {
		if err2 := exec.Command("docker", "rm", "-f", dockerid).Run(); err2 != nil {
			t.Fatalf("failed to stop %s: %s", image, err2)
		}
	}()

	bus, err := NewMQTT(fmt.Sprintf("tcp://127.0.0.1:%d", p), "test", "", "", Message{}, true)
	if err != nil {
		t.Fatal(err)
	}

	// TODO:
	// Subscribe
	// Publish
	// (retrieve)
	// Stop
	// Start
	// Publish
	// (retrieve?)

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
