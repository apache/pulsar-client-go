module github.com/apache/pulsar-client-go

go 1.13

require (
	github.com/DataDog/zstd v1.4.6-0.20200617134701-89f69fb7df32
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20200715083626-b9f8c5cedefb
	github.com/beefsack/go-rate v0.0.0-20180408011153-efa7637bb9b6
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b
	github.com/davecgh/go-spew v1.1.1
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.2
	github.com/golang/snappy v0.0.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/compress v1.10.8
	github.com/kr/pretty v0.2.0 // indirect
	github.com/linkedin/goavro v2.1.0+incompatible
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/pierrec/lz4 v2.0.5+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/sirupsen/logrus v1.4.2
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/yahoo/athenz v1.8.55
)

replace github.com/apache/pulsar-client-go/oauth2 => ./oauth2
