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

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"

	"github.com/spf13/cobra"

	log "github.com/sirupsen/logrus"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// FlagProfile is a global flag
var FlagProfile bool
var flagDebug bool
var PrometheusPort int

type ClientArgs struct {
	ServiceURL              string
	TokenFile               string
	TLSTrustCertFile        string
	MaxConnectionsPerBroker int
}

var clientArgs ClientArgs

func NewClient() (pulsar.Client, error) {
	clientOpts := pulsar.ClientOptions{
		URL:                     clientArgs.ServiceURL,
		MaxConnectionsPerBroker: clientArgs.MaxConnectionsPerBroker,
	}

	if clientArgs.TokenFile != "" {
		// read JWT from the file
		tokenBytes, err := ioutil.ReadFile(clientArgs.TokenFile)
		if err != nil {
			log.WithError(err).Errorf("failed to read Pulsar JWT from a file %s", clientArgs.TokenFile)
			os.Exit(1)
		}
		clientOpts.Authentication = pulsar.NewAuthenticationToken(string(tokenBytes))
	}

	if clientArgs.TLSTrustCertFile != "" {
		clientOpts.TLSTrustCertsFilePath = clientArgs.TLSTrustCertFile
	}
	return pulsar.NewClient(clientOpts)
}

func initLogger(debug bool) {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "15:04:05.000",
	})
	level := log.InfoLevel
	if debug {
		level = log.DebugLevel
	}
	log.SetLevel(level)
}

func main() {
	rootCmd := &cobra.Command{
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			initLogger(flagDebug)
		},
		Use: "pulsar-perf-go",
	}

	flags := rootCmd.PersistentFlags()
	flags.BoolVar(&FlagProfile, "profile", false, "enable profiling")
	flags.IntVar(&PrometheusPort, "metrics", 8000, "Port to use to export metrics for Prometheus. Use -1 to disable.")
	flags.BoolVar(&flagDebug, "debug", false, "enable debug output")
	flags.StringVarP(&clientArgs.ServiceURL, "service-url", "u",
		"pulsar://localhost:6650", "The Pulsar service URL")
	flags.StringVar(&clientArgs.TokenFile, "token-file", "", "file path to the Pulsar JWT file")
	flags.StringVar(&clientArgs.TLSTrustCertFile, "trust-cert-file", "", "file path to the trusted certificate file")
	flags.IntVarP(&clientArgs.MaxConnectionsPerBroker, "max-connections", "c", 1,
		"Max connections to open to broker. Defaults to 1.")

	rootCmd.AddCommand(newProducerCommand())
	rootCmd.AddCommand(newConsumerCommand())

	if PrometheusPort > 0 {
		go func() {
			log.Info("Starting Prometheus metrics at http://localhost:", PrometheusPort, "/metrics")
			http.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(":"+strconv.Itoa(PrometheusPort), nil)
		}()
	}

	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "executing command error=%+v\n", err)
		os.Exit(1)
	}
}

func stopCh() <-chan struct{} {
	stop := make(chan struct{})
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		close(stop)
	}()
	return stop
}

func RunProfiling(stop <-chan struct{}) {
	go func() {
		if err := serveProfiling("0.0.0.0:6060", stop); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Error("Unable to start debug profiling server")
		}
	}()
}

// use `http://addr/debug/pprof` to access the browser
// use `go tool pprof http://addr/debug/pprof/profile` to get pprof file(cpu info)
// use `go tool pprof http://addr/debug/pprof/heap` to get inuse_space file
func serveProfiling(addr string, stop <-chan struct{}) error {
	s := http.Server{
		Addr:    addr,
		Handler: http.DefaultServeMux,
	}
	go func() {
		<-stop
		log.Infof("Shutting down pprof server")
		s.Shutdown(context.Background())
	}()

	fmt.Printf("Starting pprof server at: %s\n", addr)
	fmt.Printf("  use `http://%s/debug/pprof` to access the browser\n", addr)
	fmt.Printf("  use `go tool pprof http://%s/debug/pprof/profile` to get pprof file(cpu info)\n", addr)
	fmt.Printf("  use `go tool pprof http://%s/debug/pprof/heap` to get inuse_space file\n", addr)
	fmt.Println()

	return s.ListenAndServe()
}
