# BloomD client for Golang

[![GoDoc](https://godoc.org/github.com/eduardoramirez/go-bloomd?status.svg)](https://godoc.org/github.com/eduardoramirez/go-bloomd)

A thread safe BloomD client. (https://github.com/armon/bloomd)

Requires Golang 10 or above.

Each request to bloomD will use one of the connections from the pool. A connection
is one to one with the request and thus is thread safe. Once the request is complete
the connection will be released back to the pool. There are a few configurations that
can be applied to the client, refer to `Option`.

More info about bloom filters: http://en.wikipedia.org/wiki/Bloom_filter

## Installation

Install:

```shell
go get -u github.com/eduardoramirez/go-bloomd
```

Import:

```go
import "github.com/eduardoramirez/go-bloomd"
```

## Quickstart

```go
import (
  "context"

  "github.com/eduardoramirez/go-bloomd"
)

func ExampleNewClient() {
	client := bloomd.NewClient("localhost:8673", bloomd.WithHashKeys(true), bloomd.WithMaxAttempts(5))

	err := client.Ping()
  if err != nil {
    panic(err)
  }

  client.Shutdown()
}

func ExampleClient() {
  ctx := context.Background()
  client := bloomd.NewClient("localhost:8673")

  client.Create(ctx, "testFilter")

  r, err := client.Check(ctx, "testFilter", "Key")
  if err != nil {
    panic(err)
  }
  // r: false

  client.Set(ctx, "testFilter", "Key")
  r, err = client.Check(ctx, "testFilter", "Key")
  if err != nil {
    panic(err)
  }
  // r: true
}
```

## Client Options

A number of config options are available for the client:

* ```hashKeys```: Whether to hash the keys before sending them over to bloomD. Defaults to false.
* ```initialConnections```: The number of connections the pool will be initialized with. Defaults to 5.
* ```maxConnections```: The number of maximum connections the pool will have at any given time. Defaults to 10.
* ```maxAttempts```: The number of retries when communicating to bloomD. Defaults to 3.
* ```timeout```: How long to wait for bloomD to reply before returning an error. Defaults to 10 seconds.

## Credits

 * Forked from [go-bloomd](https://github.com/sjhitchner/go-bloomd) by [Stephen Hitchner](https://github.com/sjhitchner)

## License

The MIT License (MIT)
