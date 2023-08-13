package integration

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// integrationImage = `docker.io/apachepulsar/pulsar:3.0.0`
	integrationImage = `docker.io/apachepulsar/pulsar:2.10.3`
	pulsarCommand    = `bin/pulsar`
)

var pulsarArgs = []string{
	"standalone",
	"--no-functions-worker",
	"--no-stream-storage",
}

var containerEnv = map[string]string{
	"PULSAR_EXTRA_OPTS": "-Dpulsar.auth.basic.conf=/pulsar/conf/.htpasswd",
}

var containerFiles = []string{
	"certs/broker-cert.pem",
	"certs/broker-key.pem",
	"certs/cacert.pem",
	"certs/client-cert.pem",
	"certs/client-key.pem",
	"tokens/secret.key",
	"tokens/token.txt",
	"conf/.htpasswd",
	"conf/client.conf",
	"conf/standalone.conf",
}

func integContainerConfig(cfg *tc.GenericContainerRequest) {
	cfg.Name = "pulsar-client-go-test"
	cfg.ExposedPorts = append(
		cfg.ExposedPorts,
		"8443/tcp",
		"6651/tcp",
	)
	cfg.Cmd = append([]string{pulsarCommand}, pulsarArgs...)
	for k, v := range containerEnv {
		cfg.Env[k] = v
	}
	for _, dir := range []string{"certs", "conf", "tokens"} {
		cfg.Files = append(cfg.Files, tc.ContainerFile{
			HostFilePath:      MustFilePath(dir),
			ContainerFilePath: filepath.Join("/pulsar", dir),
			FileMode:          0o777,
		})
	}
	cfg.WaitingFor = wait.ForLog("Successfully updated the policies on namespace public/default")
	// for _, file := range containerFiles {
	// 	cfg.Files = append(cfg.Files, tc.ContainerFile{
	// 		HostFilePath:      MustFilePath(file),
	// 		ContainerFilePath: filepath.Join("/pulsar", file),
	// 		FileMode:          0o777,
	// 	})
	// }
}

var pulsarAdminCommands = []string{
	`tenants update public -r anonymous`,
	`namespaces grant-permission public/default --actions produce,consume --role anonymous`,
	`tenants create private`,
	`namespaces create private/auth`,
	`namespaces grant-permission private/auth --actions produce,consume --role token-principal`,
}

var (
	pulsarContainer tc.Container
	containerOnce   sync.Once
)

func Container() tc.Container {
	containerOnce.Do(func() {
		req := tc.ContainerRequest{
			Name:  "pulsar-client-go-test",
			Image: integrationImage,
			ExposedPorts: []string{
				"6650/tcp",
				"8080/tcp",
				"6651/tcp",
				"8443/tcp",
			},
			Env: map[string]string{
				"PULSAR_EXTRA_OPTS": "-Dpulsar.auth.basic.conf=/pulsar/conf/.htpasswd",
			},
			Cmd: []string{
				"bin/pulsar",
				"standalone",
				"--no-functions-worker",
				"--no-stream-storage",
			},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.AutoRemove = true
			},
			WaitingFor: wait.ForAll(
				wait.ForHTTP("/admin/v2/clusters").WithPort("8080/tcp").WithResponseMatcher(func(r io.Reader) bool {
					respBytes, _ := io.ReadAll(r)
					resp := string(respBytes)
					return resp == `["standalone"]`
				}),
				// Successfully granted access for role token-principal:
			),
		}
		for _, dir := range []string{"certs", "conf", "tokens"} {
			req.Files = append(req.Files, tc.ContainerFile{
				HostFilePath:      MustFilePath(dir),
				ContainerFilePath: filepath.Join("/pulsar", dir),
				FileMode:          0o777,
			})
		}
		c, err := tc.GenericContainer(context.Background(), tc.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to start pulsar container: %v", err)
			runtime.Goexit()
		}
		for _, adminCmd := range pulsarAdminCommands {
			cmd := strings.Split(`/pulsar/bin/pulsar-admin `+adminCmd, " ")
			code, r, err := c.Exec(context.Background(), cmd)
			if err != nil || code != 0 {
				failure, _ := io.ReadAll(r)
				fmt.Fprintf(os.Stderr, "failed to run cmd %q (code %d):\n%s", cmd, code, string(failure))
				runtime.Goexit()
			}
		}

		// c, err := tcpulsar.RunContainer(
		// 	context.Background(),
		// 	tc.WithImage(integrationImage),
		// 	tc.CustomizeRequestOption(integContainerConfig),
		// )

		pulsarContainer = c
	})
	return pulsarContainer
}

func URL(proto, port string, path ...string) string {
	if !strings.HasSuffix(proto, "://") {
		proto += "://"
	}
	externalPort := ExternalPort(port)
	url := fmt.Sprintf(`%s%s:%s`, proto, "localhost", externalPort)
	if len(path) > 0 {
		url += `/` + strings.Join(path, `/`)
	}
	return url
}

func ExternalPort(internalPort string) string {
	mapped, _ := Container().MappedPort(context.Background(), nat.Port(internalPort))
	return mapped.Port()
}

func isDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		panic("somehow failed to find a file: " + err.Error())
	}
	return s.IsDir()
}
