package bloomd

import (
	"context"
	"net"
)

type Filter string

type Client interface {
	Check(context.Context, Filter, string) (bool, error)
	Clear(context.Context, Filter) error
	Close(context.Context, Filter) error
	Create(context.Context, Filter) error
	Drop(context.Context, Filter) error
	Flush(context.Context) error
	List(context.Context) (map[string]string, error)
	MultiCheck(context.Context, Filter, ...string) ([]bool, error)
	MultiSet(context.Context, Filter, ...string) ([]bool, error)
	Set(context.Context, Filter, string) (bool, error)
	Info(context.Context, Filter) (map[string]string, error)
	Shutdown()
	Ping() error
}

type Pool interface {
	Get() (net.Conn, error)
	Close()
	Len() int
}

const (
	FLUSH           = "flush"
	LIST            = "list"
	CREATE          = "create %s"
	CREATE_CAPACITY = "%s capacity=%d"
	CREATE_PROB     = "%s prob=%f"
	CREATE_INMEM    = "%s in_memory=1"
	DROP            = "drop %s"
	CLOSE           = "close %s"
	CLEAR           = "clear %s"
	MULTI           = "m"
	BULK            = "b"
	SET             = "s %s %s"
	INFO            = "info %s"

	RESPONSE_DONE   = "Done"
	RESPONSE_EXISTS = "Exists"
	RESPONSE_YES    = "Yes"
	RESPONSE_NO     = "No"
	RESPONSE_START  = "START"
	RESPONSE_END    = "END"
)

type singlePool struct {
	hostname string
}

// Single connection "pool"
func (t *singlePool) Get() (net.Conn, error) {
	return net.Dial("tcp", t.hostname)
}

func (t *singlePool) Close() {}

func (t *singlePool) Len() int {
	return 1
}
