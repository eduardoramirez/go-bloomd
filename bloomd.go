package bloomd

import (
	"bufio"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	pool "gopkg.in/fatih/pool.v2"
)

type Filter string

type Client interface {
	Bulk(context.Context, Filter, ...string) ([]bool, error)
	Check(context.Context, Filter, string) (bool, error)
	Clear(context.Context, Filter) error
	Close(context.Context, Filter) error
	Create(context.Context, Filter) error
	Drop(context.Context, Filter) error
	Flush(context.Context) error
	Info(context.Context, Filter) (map[string]string, error)
	Multi(context.Context, Filter, ...string) ([]bool, error)
	List(context.Context) (map[string]string, error)
	Set(context.Context, Filter, string) (bool, error)
	Shutdown()
	Ping() error
}

const (
	defaultInitialConnections = 5
	defaultHashKeys           = false
	defaultMaxAttempts        = 3
	defaultMaxConnections     = 10
	defaultTimeout            = time.Second * 10

	_FLUSH           = "flush"
	_LIST            = "list"
	_CHECK           = "c"
	_CREATE          = "create %s"
	_CREATE_CAPACITY = "%s capacity=%d"
	_CREATE_PROB     = "%s prob=%f"
	_CREATE_INMEM    = "%s in_memory=1"
	_DROP            = "drop %s"
	_CLOSE           = "close %s"
	_CLEAR           = "clear %s"
	_MULTI           = "m"
	_BULK            = "b"
	_SET             = "s"
	_INFO            = "info %s"

	_RESPONSE_DONE   = "Done"
	_RESPONSE_EXISTS = "Exists"
	_RESPONSE_YES    = "Yes"
	_RESPONSE_NO     = "No"
	_RESPONSE_START  = "START"
	_RESPONSE_END    = "END"
)

type channelPool interface {
	Get() (net.Conn, error)
	Close()
}

type client struct {
	pool        channelPool
	hostname    string
	timeout     time.Duration
	maxAttempts int
	hashKeys    bool
}

func NewClient(hostname string, opts ...Option) (*client, error) {
	o := evaluateOptions(opts)

	pool, err := pool.NewChannelPool(o.initialConnections, o.maxConnections, func() (net.Conn, error) {
		return net.Dial("tcp", hostname)
	})

	if err != nil {
		return nil, errors.Wrap(err, "Unable to create bloomd connection")
	}

	return &client{
		pool:        pool,
		hostname:    hostname,
		timeout:     o.timeout,
		maxAttempts: o.maxAttempts,
		hashKeys:    o.hashKeys,
	}, nil
}

// Add new key to filter
func (t client) Set(ctx context.Context, name Filter, key string) (bool, error) {
	return t.sendSingleCommand(ctx, _SET, name, key)
}

// Add multiple keys in the filter
func (t client) Bulk(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	return t.sendMultiCommand(ctx, _BULK, name, keys...)
}

// Check if key exists in filter
func (t client) Check(ctx context.Context, name Filter, key string) (bool, error) {
	return t.sendSingleCommand(ctx, _CHECK, name, key)
}

// Checks whether multiple keys exist in the filter
func (t client) Multi(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	return t.sendMultiCommand(ctx, _MULTI, name, keys...)
}

// Creates new fiter
func (t client) Create(ctx context.Context, name Filter) error {
	return t.CreateWithParams(ctx, name, 0, 0, false)
}

// Creates new fiter with additional params
func (t client) CreateWithParams(ctx context.Context, name Filter, capacity int, probability float64, inMemory bool) error {
	if probability > 0 && capacity < 1 {
		return errors.New("invalid capacity/probability")
	}

	cmd := fmt.Sprintf(_CREATE, name)
	if capacity > 0 {
		cmd = fmt.Sprintf(_CREATE_CAPACITY, cmd, capacity)
	}
	if probability > 0 {
		cmd = fmt.Sprintf(_CREATE_PROB, cmd, probability)
	}
	if inMemory {
		cmd = fmt.Sprintf(_CREATE_INMEM, cmd)
	}

	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return err
	}

	switch resp {
	case _RESPONSE_DONE:
		return nil

	case _RESPONSE_EXISTS:
		return nil

	default:
		return errors.Errorf("Invalid create resp '%s'", resp)
	}
}

// Retrieves information about the specified filter
func (t client) Info(ctx context.Context, name Filter) (map[string]string, error) {
	return t.sendBlockCommand(ctx, fmt.Sprintf(_INFO, name))
}

// Permanently deletes filter
func (t client) Drop(ctx context.Context, name Filter) error {
	return t.sendDoneCommand(ctx, fmt.Sprintf(_DROP, name))
}

// Clears the filter
func (t client) Clear(ctx context.Context, name Filter) error {
	return t.sendDoneCommand(ctx, fmt.Sprintf(_CLEAR, name))
}

// Closes the filter
func (t client) Close(ctx context.Context, name Filter) error {
	return t.sendDoneCommand(ctx, fmt.Sprintf(_CLOSE, name))
}

// Lists all filters
func (t client) List(ctx context.Context) (map[string]string, error) {
	return t.sendBlockCommand(ctx, _LIST)
}

// Flushes filters to disk
func (t client) Flush(ctx context.Context) error {
	return t.sendDoneCommand(ctx, _FLUSH)
}

func (t client) Shutdown() {
	t.pool.Close()
}

func (t client) Ping() error {
	ctx := context.Background()
	_, err := t.List(ctx)
	return err
}

// Returns the key we should send to the server
func (t client) hashKey(key string) string {
	if t.hashKeys {
		h := sha1.New()
		io.WriteString(h, key)
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	return key
}

func (t client) sendDoneCommand(ctx context.Context, cmd string) error {
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return errors.Wrapf(err, "bloomd: error with command '%s'", cmd)
	}

	if resp != _RESPONSE_DONE {
		return errors.Errorf("bloomd: error with resp '%s' command '%s'", resp, cmd)
	}

	return nil
}

func (t client) sendSingleCommand(ctx context.Context, c string, name Filter, key string) (bool, error) {
	cmd := t.buildCommand(c, name, key)

	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return false, errors.Wrapf(err, "bloomd: error with single command '%s'", cmd)
	}

	switch resp {
	case _RESPONSE_YES:
		return true, nil

	case _RESPONSE_NO:
		return false, nil

	default:
		return false, errors.Errorf("bloomd: invalid response '%s'", resp)
	}
}

func (t client) sendMultiCommand(ctx context.Context, c string, name Filter, keys ...string) ([]bool, error) {
	cmd := t.buildCommand(c, name, keys...)

	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return nil, errors.Wrapf(err, "bloomd: error with multi command '%s'", cmd)
	}

	if !strings.HasPrefix(resp, _RESPONSE_YES) && !strings.HasPrefix(resp, _RESPONSE_NO) {
		return nil, errors.Errorf("bloomd: error with multi response '%s' command '%s'", resp, cmd)
	}

	results := make([]bool, 0, len(keys))
	for _, r := range strings.Split(resp, " ") {
		results = append(results, r == _RESPONSE_YES)
	}

	return results, nil
}

func (t client) sendBlockCommand(ctx context.Context, cmd string) (map[string]string, error) {
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}

	responses := make(map[string]string)
	for _, line := range strings.Split(resp, "\n") {
		if line := strings.TrimSpace(line); line != "" {
			split := strings.SplitN(line, " ", 2)
			if len(split) != 2 {
				continue
			}
			responses[split[0]] = split[1]
		}
	}
	return responses, nil
}

func (t client) buildCommand(cmd string, name Filter, keys ...string) string {
	bldr := &strings.Builder{}
	bldr.WriteString(cmd)
	bldr.WriteRune(' ')
	bldr.WriteString(string(name))
	for _, key := range keys {
		bldr.WriteRune(' ')
		bldr.WriteString(t.hashKey(key))
	}
	return bldr.String()
}

func (t client) sendCommand(ctx context.Context, cmd string) (string, error) {
	var conn net.Conn
	var err error
	var line string

	ch := make(chan error, 1)

	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	go func() {
		ch <- func() error {
			conn, err = t.pool.Get()
			if err != nil {
				return err
			}
			defer conn.Close()

			if err = send(conn, cmd, t.maxAttempts); err != nil {
				return checkConnectionError(conn, err)
			}

			line, err = recv(conn)
			if err != nil {
				return checkConnectionError(conn, err)
			}

			return nil
		}()
	}()

	select {
	case err := <-ch:
		if err != nil {
			return "", checkConnectionError(conn, err)
		}
		return line, nil

	case <-ctx.Done():
		return "", checkConnectionError(conn, ctx.Err())
	}
}

func send(w io.Writer, cmd string, maxAttempts int) error {
	attempted := 0

	var err error
	for attempted < maxAttempts {
		if _, err = w.Write([]byte(cmd + "\n")); err == nil {
			return nil
		}
		attempted++
	}

	return errors.Wrap(err, "bloomd: unable to write to connection")
}

func recv(r io.Reader) (string, error) {
	bldr := &strings.Builder{}
	reader := bufio.NewReader(r)

	txt, err := reader.ReadString('\n')
	if err != nil {
		return "", errors.Wrap(err, "bloomd: unable to read connection")
	}

	if strings.HasPrefix(txt, _RESPONSE_START) {
		for {
			blockTxt, err := reader.ReadString('\n')
			if err != nil {
				return "", errors.Wrap(err, "bloomd: unable to read connection")
			}

			if strings.HasPrefix(blockTxt, _RESPONSE_END) {
				break
			} else {
				bldr.WriteString(blockTxt)
				bldr.WriteRune('\n')
			}
		}
	} else {
		bldr.WriteString(txt)
	}

	return strings.TrimRight(bldr.String(), "\r\n"), nil
}

func checkConnectionError(conn net.Conn, err error) error {
	if conn == nil {
		return err
	}

	if pconn, ok := conn.(*pool.PoolConn); ok {
		pconn.MarkUnusable()
	}
	return err
}
