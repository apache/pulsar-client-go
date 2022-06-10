module github.com/skulkarni-ns/pulsar-client-go

go 1.15

require (
	github.com/AthenZ/athenz v1.10.39
	github.com/DataDog/zstd v1.5.0
	github.com/skulkarni-ns/pulsar-client-go/oauth2 v0.0.0-20220120090717-25e59572242e
	github.com/beefsack/go-rate v0.0.0-20220214233405-116f4ca011a0
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b
	github.com/davecgh/go-spew v1.1.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.1.2
	github.com/klauspost/compress v1.14.4
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pierrec/lz4 v2.0.5+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.1
	github.com/sirupsen/logrus v1.6.0
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cobra v1.2.1
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.7.0
	golang.org/x/oauth2 v0.0.0-20210402161424-2e8d93401602
)

replace github.com/skulkarni-ns/pulsar-client-go/oauth2 => ./oauth2
