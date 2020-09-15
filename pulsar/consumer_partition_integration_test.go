// +build integration

package pulsar

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const image = "apachepulsar/pulsar:2.6.1"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestDeadlock(t *testing.T) {
	// Bootstrapping
	containerName := randomName("pulsar-test")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	require.NoError(t, startPulsarContainer(ctx, t, containerName), "Could not start Pulsar container")
	cancel()

	t.Log("Bootstrap done, starting test...")

	// Creating client
	ctx = context.Background()
	port := hostPort(ctx, t, containerName, "6650")
	nc, err := newClient(ClientOptions{
		URL:                     "pulsar://localhost:" + port,
		MaxConnectionsPerBroker: 1,
	})
	require.NoError(t, err)

	// Creating consumer
	topic := randomName("topic-name")
	c, err := nc.Subscribe(ConsumerOptions{
		Name:                        randomName("consumer-name"),
		SubscriptionName:            randomName("subscription-name"),
		Topic:                       topic,
		Type:                        KeyShared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	require.NoError(t, err)

	// Creating producer
	p, err := nc.CreateProducer(ProducerOptions{
		Topic: topic,
		Name:  randomName("producer-name"),
	})
	require.NoError(t, err)

	// Publishing a lot of messages to target topic
	tot := 100000
	t.Logf("Publishing %d messages", tot)
	for i := 0; i < tot; i++ {
		key := fmt.Sprintf("msg-%d", i)
		publish(ctx, t, p, key)
	}

	// Starting consumer
	wg := sync.WaitGroup{}
	wg.Add(tot) // Complete test once all messages have been consumed

	go func() {
		t.Log("Starting consumer routine")
		for i := 1; i <= tot; i++ {
			msg, err := c.Receive(ctx)
			require.NoError(t, err)
			t.Logf("Got message %d", i)

			go func(msg Message) {
				defer wg.Done()
				acknowledged := make(chan struct{})
				ticker := time.NewTicker(150 * time.Millisecond)

				go func() {
					c.Ack(msg)
					acknowledged <- struct{}{}
				}()

				for {
					select {
					case <-ticker.C:
						t.Logf("Trying to ack %+v", msg)
					case <-acknowledged:
						return
					}
				}
			}(msg)
		}
	}()

	// Abruptly close connection at intervals to simulate behaviour of messages with too big frame size
	for i := 0; i < 5; i++ {
		for _, pc := range c.(*consumer).consumers {
			pc.conn.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Wait...
	t.Log("Waiting for group to finish")
	wg.Wait()
	t.Log("Test complete!")
}

func publish(ctx context.Context, t *testing.T, p Producer, key string) {
	t.Helper()
	_, err := p.Send(ctx, &ProducerMessage{
		Payload:   []byte("ciao"),
		Key:       key,
		EventTime: time.Now(),
	})
	require.NoError(t, err)
}

func startPulsarContainer(ctx context.Context, t *testing.T, containerName string) error {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _, _ = execCommand(ctx, "docker", nil, "stop", containerName)
	})

	t.Logf("Starting container")
	_, _, err := execCommand(ctx, "docker", nil, "run",
		"--name", containerName,
		"--detach", "--rm",
		"--publish", "6650",
		"--publish", "8080", // for health checks
		image,
		"bin/pulsar", "standalone",
	)
	if err != nil {
		return err
	}

	go func(containerName string) {
		t.Logf("Following logs")
		stdout, stderr, err := execCommand(ctx, "docker", nil, "logs", "--follow", containerName)
		if err != nil {
			t.Logf("Could not follow container logs: %v", err)
			return
		}

		go func() { _, _ = io.Copy(os.Stderr, stderr) }()
		go func() { _, _ = io.Copy(os.Stdout, stdout) }()
	}(containerName)

	healthPort := hostPort(ctx, t, containerName, "8080")
	endpoint := "http://localhost:" + healthPort + "/admin/v2/brokers/health"
	t.Logf("Doing health checks on endpoint %s", endpoint)

	for remainingOK := 2; remainingOK > 0; {
		health, _, err := execCommand(ctx,
			"curl", nil,
			"--silent",
			"--retry", "60",
			"--retry-delay", "1",
			"--retry-connrefused",
			endpoint,
		)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(time.Second):
			// Sleep anyway since Pulsar can't accept connections immediately after the first couple health OKs
			if health.String() == "ok" {
				remainingOK--
			}
		}
	}

	return nil
}

func hostPort(ctx context.Context, t *testing.T, containerName, guestPort string) string {
	t.Helper()
	stdout, _, err := execCommand(ctx, "docker", nil,
		"inspect", containerName,
		"-f", `'{{ (index (index .NetworkSettings.Ports "`+guestPort+`/tcp") 0).HostPort }}'`)
	require.NoErrorf(t, err, "Could not retrieve host port for container %s:%s", containerName, guestPort)

	port := strings.ReplaceAll(stdout.String(), "'", "")
	return strings.ReplaceAll(port, "\n", "")
}

func execCommand(ctx context.Context, name string, env []string, args ...string) (
	stdout, stderr *bytes.Buffer, err error,
) {
	stdout, stderr = &bytes.Buffer{}, &bytes.Buffer{}

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = env

	if err = cmd.Start(); err != nil {
		return
	}
	if err = cmd.Wait(); err != nil {
		err = fmt.Errorf("%s: %w: %s", name, err, stderr.String())
		return
	}

	return
}

func randomName(prefix string) string {
	return fmt.Sprintf("%s-%x", prefix, rand.Int63())
}
