// +build integration

package pulsar

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/stretchr/testify/require"
)

const image = "apachepulsar/pulsar:2.6.1"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestDeadlock(t *testing.T) {
	// Bootstrapping
	containerName := randomName("pulsar-test")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	require.NoError(t, startPulsarContainer(ctx, t, containerName), "Could not start Pulsar container")
	cancel()

	t.Log("Bootstrap done, starting test...")

	// New context
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	// Debug server
	r := chi.NewRouter()
	r.Mount("/debug", middleware.Profiler())
	httpServer := http.Server{Addr: ":8081", Handler: r}
	go func() {
		t.Log("Starting debug HTTP server on 8081")
		if err := httpServer.ListenAndServe(); err != nil {
			t.Fatal("Could not start debug HTTP server")
		}
	}()

	// Creating client
	port := hostPort(ctx, t, containerName, "6650")
	nc, err := newClient(ClientOptions{
		URL:                     "pulsar://localhost:" + port,
		MaxConnectionsPerBroker: 1,
	})
	require.NoError(t, err)

	// Creating producer
	topic := randomName("topic-name")
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

	// Creating consumer
	c, err := nc.Subscribe(ConsumerOptions{
		Name:                        randomName("consumer-name"),
		SubscriptionName:            randomName("subscription-name"),
		Topic:                       topic,
		Type:                        KeyShared,
		SubscriptionInitialPosition: SubscriptionPositionEarliest,
	})
	require.NoError(t, err)

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
						t.Logf("[%s] Trying to ack %+v", time.Now(), msg.ID())
					case <-acknowledged:
						return
					}
				}
			}(msg)
		}
	}()

	// Abruptly close connection at intervals to simulate behaviour of messages with too big frame size
	for i := 0; i < 10; i++ {
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
	t.Logf("Message published: %s", key)
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
		t.Log("Following logs")
		defer t.Log("Stopped following logs")
		err := streamCommand(context.Background(), "docker", nil, "logs", "--follow", containerName)
		if err != nil {
			t.Logf("Could not follow container logs: %v", err)
			return
		}
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

func streamCommand(ctx context.Context, name string, env []string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}
	scanReader(stdout)
	go scanReader(stderr)

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}

	return nil
}

func scanReader(r io.Reader) {
	reader := bufio.NewReader(r)
	line, err := reader.ReadString('\n')
	for err == nil {
		fmt.Print(line)
		line, err = reader.ReadString('\n')
	}
}

func randomName(prefix string) string {
	return fmt.Sprintf("%s-%x", prefix, rand.Int63())
}
