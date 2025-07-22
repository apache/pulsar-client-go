// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Global metrics server for all tests
var (
	metricsServer *http.Server
	metricsPort   = 8801
	metricsMutex  sync.Mutex
)

// startMetricsServer starts a global Prometheus metrics server
func startMetricsServer() error {
	metricsMutex.Lock()
	defer metricsMutex.Unlock()

	if metricsServer != nil {
		return nil // Already started
	}

	// Check if port is available
	addr := fmt.Sprintf("localhost:%d", metricsPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("port %d is not available: %v", metricsPort, err)
	}
	ln.Close()

	// Start metrics server
	http.Handle("/metrics", promhttp.Handler())
	metricsServer = &http.Server{
		Addr: fmt.Sprintf(":%d", metricsPort),
	}

	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(fmt.Sprintf("Failed to start metrics server: %v", err))
		}
	}()

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)
	return nil
}

// stopMetricsServer stops the global Prometheus metrics server
func stopMetricsServer() error {
	metricsMutex.Lock()
	defer metricsMutex.Unlock()

	if metricsServer == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := metricsServer.Shutdown(ctx)
	metricsServer = nil
	return err
}

// getMetricValue retrieves a metric value from the Prometheus metrics server
func getMetricValue(metricName string) (float64, error) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/metrics", metricsPort))
	if err != nil {
		return 0, fmt.Errorf("failed to fetch metrics: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		// Skip comment lines that start with #
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if strings.Contains(line, metricName) {
			// Parse the metric line to extract the value
			// Format: metric_name{label="value",label2="value2"} 10
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				var value float64
				_, err := fmt.Sscanf(parts[len(parts)-1], "%f", &value)
				if err != nil {
					return 0, fmt.Errorf("failed to parse metric value: %v", err)
				}
				return value, nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("failed to scan metrics response: %v", err)
	}

	return 0, fmt.Errorf("metric %s not found in Prometheus metrics response", metricName)
}

// TestMain runs before all tests and after all tests
func TestMain(m *testing.M) {
	// Start metrics server before all tests
	if err := startMetricsServer(); err != nil {
		panic(fmt.Sprintf("Failed to start metrics server: %v", err))
	}

	// Run all tests
	code := m.Run()

	// Stop metrics server after all tests
	if err := stopMetricsServer(); err != nil {
		fmt.Printf("Failed to stop metrics server: %v\n", err)
	}

	// Exit with test result code
	os.Exit(code)
}
