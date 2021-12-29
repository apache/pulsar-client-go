module github.com/streamnative/pulsar-admin-go

go 1.13

require (
	github.com/99designs/keyring v1.1.6
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20211108044248-fe3b7c4e445b
	github.com/docker/go-connections v0.4.0
	github.com/fatih/color v1.7.0
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible
	github.com/ghodss/yaml v1.0.0
	github.com/go-sql-driver/mysql v1.5.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.4.3
	github.com/gorilla/mux v1.7.4 // indirect
	github.com/imdario/mergo v0.3.8
	github.com/kr/pretty v0.2.0 // indirect
	github.com/kris-nova/logger v0.0.0-20181127235838-fd0d87064b06
	github.com/kris-nova/lolgopher v0.0.0-20180921204813-313b3abb0d9b
	github.com/magiconair/properties v1.8.0
	github.com/mattn/go-colorable v0.1.2 // indirect
	github.com/mattn/go-runewidth v0.0.4 // indirect
	github.com/olekukonko/tablewriter v0.0.1
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.4.2 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.5.1
	github.com/testcontainers/testcontainers-go v0.0.10
	golang.org/x/net v0.0.0-20210220033124-5f55cee0dc0d // indirect
	golang.org/x/oauth2 v0.0.0-20210220000619-9bb904979d93
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.3.0
)

replace github.com/apache/pulsar-client-go/oauth2 => github.com/apache/pulsar-client-go/oauth2 v0.0.0-20211006154457-742f1b107403
