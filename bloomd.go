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

// A wrapper type for a bloomD filter name.
type Filter string

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

// Client is represention of a configured client to a bloomD server.
type Client struct {
	pool        channelPool
	hostname    string
	timeout     time.Duration
	maxAttempts int
	hashKeys    bool
}

// NewClient returns a new bloomD client configured according to the options
// or using the default settings.
func NewClient(hostname string, opts ...Option) (*Client, error) {
	o := evaluateOptions(opts)

	pool, err := pool.NewChannelPool(o.initialConnections, o.maxConnections, func() (net.Conn, error) {
		return net.Dial("tcp", hostname)
	})

	if err != nil {
		return nil, errors.Wrap(err, "Unable to create bloomd connection")
	}

	return &Client{
		pool:        pool,
		hostname:    hostname,
		timeout:     o.timeout,
		maxAttempts: o.maxAttempts,
		hashKeys:    o.hashKeys,
	}, nil
}

// Set sets a key in a filter.
func (t *Client) Set(ctx context.Context, name Filter, key string) (bool, error) {
	return t.sendSingleCommand(ctx, _SET, name, key)
}

// Bulk sets many items in a filter at once.
func (t *Client) Bulk(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	return t.sendMultiCommand(ctx, _BULK, name, keys...)
}

// Check checks if a key is in a filter.
func (t *Client) Check(ctx context.Context, name Filter, key string) (bool, error) {
	return t.sendSingleCommand(ctx, _CHECK, name, key)
}

// Multi checks whether multiple keys exist in the filter.
func (t *Client) Multi(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	return t.sendMultiCommand(ctx, _MULTI, name, keys...)
}

// Create a new filter (a filter is a named bloom filter).
func (t *Client) Create(ctx context.Context, name Filter) error {
	return t.CreateWithParams(ctx, name, 0, 0, false)
}

// CreateWithParams creates a new filter with the given properties.
func (t *Client) CreateWithParams(ctx context.Context, name Filter, capacity int, probability float64, inMemory bool) error {
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

// Info retrieves information about the specified filter.
func (t *Client) Info(ctx context.Context, name Filter) (map[string]string, error) {
	return t.sendBlockCommand(ctx, fmt.Sprintf(_INFO, name))
}

// Drop permanently deletes filter.
func (t *Client) Drop(ctx context.Context, name Filter) error {
	return t.sendDoneCommand(ctx, fmt.Sprintf(_DROP, name))
}

// Clear removes a filter from memory but retains it in disk.
func (t *Client) Clear(ctx context.Context, name Filter) error {
	return t.sendDoneCommand(ctx, fmt.Sprintf(_CLEAR, name))
}

// Close closes a filter (Unmaps from memory, but still accessible).
func (t *Client) Close(ctx context.Context, name Filter) error {
	return t.sendDoneCommand(ctx, fmt.Sprintf(_CLOSE, name))
}

// List lists all filters.
func (t *Client) List(ctx context.Context) (map[string]string, error) {
	// TODO (eduardo): support prefix matching
	return t.sendBlockCommand(ctx, _LIST)
}

// Flush flushes all filters to disk or just a specified one.
func (t *Client) Flush(ctx context.Context) error {
	// TODO (eduardo): support specifying a filter
	return t.sendDoneCommand(ctx, _FLUSH)
}

// Shutdown closes every connection in the pool.
func (t *Client) Shutdown() {
	t.pool.Close()
}

// Ping hits bloomD and returns an error or nil.
func (t *Client) Ping() error {
	ctx := context.Background()
	_, err := t.List(ctx)
	return err
}

// Returns the key the client will send to the server, maybe hashing it.
func (t *Client) hashKey(key string) string {
	if t.hashKeys {
		h := sha1.New()
		io.WriteString(h, key)
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	return key
}

// sendDoneCommand sends the command to bloomD. Returns an error if the
// reply was malformed.
func (t *Client) sendDoneCommand(ctx context.Context, cmd string) error {
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return errors.Wrapf(err, "bloomd: error with command '%s'", cmd)
	}

	if resp != _RESPONSE_DONE {
		return errors.Errorf("bloomd: error with resp '%s' command '%s'", resp, cmd)
	}

	return nil
}

// sendSingleCommand builds and sends the command to bloomD. Returns the response as a boolean.
func (t *Client) sendSingleCommand(ctx context.Context, c string, name Filter, key string) (bool, error) {
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

// sendSingleCommand builds and sends the command to bloomD. Returns the response as a list of booleans.
func (t *Client) sendMultiCommand(ctx context.Context, c string, name Filter, keys ...string) ([]bool, error) {
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

// sendBlockCommand sends the command to bloomD. Returns the response key value pairs.
func (t *Client) sendBlockCommand(ctx context.Context, cmd string) (map[string]string, error) {
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

func (t *Client) buildCommand(cmd string, name Filter, keys ...string) string {
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

// sendCommand sends the command asynchronously to bloomD. Returns the parsed response.
func (t *Client) sendCommand(ctx context.Context, cmd string) (string, error) {
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

// send writes the request to bloomD. Retrying as necessary.
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

// recv retrieves the response from bloomD and parses it.
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
