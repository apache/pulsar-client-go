module github.com/apache/pulsar-client-go/pulsar-perf

go 1.15

replace github.com/apache/pulsar-client-go => ../

require (
	github.com/apache/pulsar-client-go v0.8.1
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b
	github.com/prometheus/client_golang v1.12.2
	github.com/sirupsen/logrus v1.9.0
	github.com/spf13/cobra v1.5.0
)
