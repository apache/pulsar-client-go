module github.com/apache/pulsar-client-go

go 1.13

require (
	github.com/AthenZ/athenz v1.10.15
	github.com/DataDog/zstd v1.4.6-0.20210211175136-c6db21d202f4
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20201120111947-b8bd55bc02bd
	github.com/beefsack/go-rate v0.0.0-20180408011153-efa7637bb9b6
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b
	github.com/davecgh/go-spew v1.1.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.4.2
	github.com/google/uuid v1.1.2
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/compress v1.10.8
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pierrec/lz4 v2.0.5+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/sirupsen/logrus v1.4.2
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/stretchr/testify v1.5.1
	go.uber.org/atomic v1.7.0
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
)

replace github.com/apache/pulsar-client-go/oauth2 => ./oauth2

replace github.com/apache/pulsar-client-go => github.com/1046102779/pulsar-client-go v0.6.1-0.20211020191107-81f383018c50
