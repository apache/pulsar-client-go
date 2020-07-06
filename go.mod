module github.com/apache/pulsar-client-go

go 1.12

require (
	github.com/DataDog/zstd v1.4.5
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-00010101000000-000000000000
	github.com/beefsack/go-rate v0.0.0-20180408011153-efa7637bb9b6
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b
	github.com/gogo/protobuf v1.3.1
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/klauspost/compress v1.10.5
	github.com/kr/pretty v0.2.0 // indirect
	github.com/pierrec/lz4 v2.0.5+incompatible
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.4.1
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/yahoo/athenz v1.8.55
	k8s.io/utils v0.0.0-20200619165400-6e3d28b6ed19
)

replace github.com/apache/pulsar-client-go/oauth2 => ./oauth2
