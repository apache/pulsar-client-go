module github.com/apache/pulsar-client-go

go 1.13

require (
	github.com/DataDog/zstd v1.4.6-0.20200617134701-89f69fb7df32
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20200715083626-b9f8c5cedefb
	github.com/beefsack/go-rate v0.0.0-20180408011153-efa7637bb9b6
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b
	github.com/frankban/quicktest v1.11.0 // indirect
	github.com/go-chi/chi v4.1.2+incompatible
	github.com/gogo/protobuf v1.3.1
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/compress v1.10.10
	github.com/pierrec/lz4 v2.5.2+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/sirupsen/logrus v1.6.0
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/testify v1.5.1
	github.com/yahoo/athenz v1.9.12
)

replace github.com/apache/pulsar-client-go/oauth2 => ./oauth2
