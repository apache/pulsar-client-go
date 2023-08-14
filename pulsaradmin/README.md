# Pulsar Admin Go Library

Pulsar-Admin-Go is a [Go](https://go.dev) library for [Apache Pulsar](https://pulsar.apache.org/). It provides a unified Go API for managing pulsar resources such as tenants, namespaces and topics, etc.

## Motivation

Currently, many projects (e.g, [terraform-provider-pulsar](https://github.com/streamnative/terraform-provider-pulsar) and [pulsar-resources-operator](https://github.com/streamnative/pulsar-resources-operator)) 
that need to manipulate the pulsar admin resources rely on the [pulsarctl](https://github.com/streamnative/pulsarctl), 
which poses challenges for dependency management and versioning as we have to release a new pulsarctl to get updates.
So we decoupled the pulsar admin related api from pulsarctl and created the [pulsar-admin-go](https://github.com/streamnative/pulsar-admin-go) library based on it, 
which also provides a clearer perspective and maintainability from an architectural perspective.

## Quickstart

### Prerequisite

- go1.18+
- pulsar-admin-go in go.mod

  ```shell
  go get github.com/apache/pulsar-client-go
  ```

### Manage pulsar tenants

- List all tenants

```go
import (
    "github.com/apache/pulsar-client-go/pulsaradmin"
)

func main() {
    cfg := &pulsaradmin.Config{}
    admin, err := pulsaradmin.NewClient(cfg)
    if err != nil {
        panic(err)
    }
    
    tenants, _ := admin.Tenants().List()
}
```

### Manage pulsar namespaces

- List all namespaces

```go
import (
    "github.com/apache/pulsar-client-go/pulsaradmin"
)

func main() {
    cfg := &pulsaradmin.Config{}
    admin, err := pulsaradmin.NewClient(cfg)
    if err != nil {
        panic(err)
    }
    
    namespaces, _ := admin.Namespaces().GetNamespaces("public")
}
```

- Create a new namespace

```go
import (
    "github.com/apache/pulsar-client-go/pulsaradmin"
)

func main() {
    cfg := &pulsaradmin.Config{}
    admin, err := pulsaradmin.NewClient(cfg)
    if err != nil {
        panic(err)
    }
    
    admin.Namespaces().CreateNamespace("public/dev")
}
```

### Manage pulsar topics

- Create a topic

```go

import (
    "github.com/apache/pulsar-client-go/pulsaradmin"
    "github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

func main() {
    cfg := &pulsaradmin.Config{}
    admin, err := pulsaradmin.NewClient(cfg)
    if err != nil {
        panic(err)
    }
    
    topic, _ := utils.GetTopicName("public/dev/topic")
    
    admin.Topics().Create(*topic, 3)
}
```

## Contributing

Contributions are warmly welcomed and greatly appreciated! 
The project follows the typical GitHub pull request model. See [CONTRIBUTING.md](CONTRIBUTING.md) for more details. 
Before starting any work, please either comment on an existing issue, or file a new one.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). See [LICENSE](LICENSE)