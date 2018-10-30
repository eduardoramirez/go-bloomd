package bloomd

import (
	"bufio"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/pkg/errors"
	pool "gopkg.in/fatih/pool.v2"
)

const (
	// Valid bloomD commands
	_BULK   = "b"
	_CHECK  = "c"
	_CLEAR  = "clear"
	_CLOSE  = "close"
	_CREATE = "create"
	_DROP   = "drop"
	_FLUSH  = "flush"
	_INFO   = "info"
	_LIST   = "list"
	_MULTI  = "m"
	_SET    = "s"

	// Create command optionals
	_CREATE_CAPACITY = "capacity="
	_CREATE_PROB     = "prob="
	_CREATE_INMEM    = "in_memory=1"

	// Valid bloomD block identifiers
	_RESPONSE_START = "START"
	_RESPONSE_END   = "END"
)

type channelPool interface {
	Get() (net.Conn, error)
	Close()
}

// Client is represention of a configured client to a bloomD server.
type Client struct {
	pool        channelPool
	hostname    string
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
		maxAttempts: o.maxAttempts,
		hashKeys:    o.hashKeys,
	}, nil
}

// Set sets a key in a filter.
func (t *Client) Set(ctx context.Context, name string, key string) (bool, error) {
	cmd := t.buildCommand(_SET, name, key)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return false, err
	}

	return parseBool(resp)
}

// Bulk sets many items in a filter at once.
func (t *Client) Bulk(ctx context.Context, name string, keys ...string) ([]bool, error) {
	cmd := t.buildCommand(_BULK, name, keys...)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}

	return parseBoolList(len(keys), resp)
}

// Check checks if a key is in a filter.
func (t *Client) Check(ctx context.Context, name string, key string) (bool, error) {
	cmd := t.buildCommand(_CHECK, name, key)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return false, err
	}

	return parseBool(resp)
}

// Multi checks whether multiple keys exist in the filter.
func (t *Client) Multi(ctx context.Context, name string, keys ...string) ([]bool, error) {
	cmd := t.buildCommand(_MULTI, name, keys...)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}

	return parseBoolList(len(keys), resp)
}

// Create a new filter (a filter is a named bloom filter).
func (t *Client) Create(ctx context.Context, name string) error {
	return t.CreateWithParams(ctx, name, 0, 0, false)
}

// CreateWithParams creates a new filter with the given properties.
func (t *Client) CreateWithParams(ctx context.Context, name string, capacity int, probability float64, inMemory bool) error {
	if probability > 0 && capacity < 1 {
		return errors.New("bloomd: invalid capacity/probability")
	}

	cmd := t.buildCreateCommand(name, capacity, probability, inMemory)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return err
	}

	return parseCreate(resp)
}

// Info retrieves information about the specified filter.
func (t *Client) Info(ctx context.Context, name string) (VerboseBloomFilter, error) {
	cmd := t.buildCommand(_INFO, name)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return VerboseBloomFilter{}, err
	}

	return parseInfo(name, resp)
}

// Drop permanently deletes filter.
func (t *Client) Drop(ctx context.Context, name string) error {
	cmd := t.buildCommand(_DROP, name)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return err
	}

	return parseDropConfirmation(resp)
}

// Clear removes a items from a filter
func (t *Client) Clear(ctx context.Context, name string) error {
	cmd := t.buildCommand(_CLEAR, name)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return err
	}

	return parseConfirmation(resp)
}

// Close closes a filter (Unmaps from memory, but still accessible).
func (t *Client) Close(ctx context.Context, name string) error {
	cmd := t.buildCommand(_CLOSE, name)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return err
	}

	return parseConfirmation(resp)
}

// List lists all filters.
func (t *Client) ListAll(ctx context.Context) ([]BloomFilter, error) {
	resp, err := t.sendCommand(ctx, _LIST)
	if err != nil {
		return nil, err
	}

	return parseFilterList(resp)
}

// List lists all filters that match the prefix.
func (t *Client) ListByPrefix(ctx context.Context, prefix string) ([]BloomFilter, error) {
	cmd := t.buildCommand(_LIST, prefix)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return nil, err
	}

	return parseFilterList(resp)
}

// Flush flushes all filters to disk.
func (t *Client) FlushAll(ctx context.Context) error {
	resp, err := t.sendCommand(ctx, _FLUSH)
	if err != nil {
		return err
	}

	return parseConfirmation(resp)
}

// Flush flushes the speficied filter to disk.
func (t *Client) FlushFilter(ctx context.Context, name string) error {
	cmd := t.buildCommand(_FLUSH, name)
	resp, err := t.sendCommand(ctx, cmd)
	if err != nil {
		return err
	}

	return parseConfirmation(resp)
}

// Shutdown closes every connection in the pool.
func (t *Client) Shutdown() {
	t.pool.Close()
}

// Ping hits bloomD and returns an error or nil.
func (t *Client) Ping() error {
	ctx := context.Background()
	_, err := t.ListAll(ctx)
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

func (t *Client) buildCommand(cmd string, arg string, keys ...string) string {
	bldr := &strings.Builder{}
	bldr.WriteString(cmd)
	bldr.WriteRune(' ')
	bldr.WriteString(arg)
	for _, key := range keys {
		bldr.WriteRune(' ')
		bldr.WriteString(t.hashKey(key))
	}
	return bldr.String()
}

func (t *Client) buildCreateCommand(name string, capacity int, probability float64, inMemory bool) string {
	bldr := &strings.Builder{}
	bldr.WriteString(_CREATE)
	bldr.WriteRune(' ')
	bldr.WriteString(name)
	if capacity > 0 {
		bldr.WriteRune(' ')
		bldr.WriteString(_CREATE_CAPACITY)
		bldr.WriteString(fmt.Sprintf("%d", capacity))
	}
	if probability > 0 {
		bldr.WriteRune(' ')
		bldr.WriteString(_CREATE_PROB)
		bldr.WriteString(fmt.Sprintf("%f", probability))
	}
	if inMemory {
		bldr.WriteRune(' ')
		bldr.WriteString(_CREATE_INMEM)
	}
	return bldr.String()
}

// sendCommand sends the command asynchronously to bloomD. Returns the parsed response.
func (t *Client) sendCommand(ctx context.Context, cmd string) (string, error) {
	var conn net.Conn
	var err error
	var line string

	errCh := make(chan error, 1)

	go func() {
		errCh <- func() error {
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
	case err := <-errCh:
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

// recv retrieves the response from bloomD.
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
			}
		}
	} else {
		bldr.WriteString(txt)
	}

	// Strip out the last newline
	responseString := bldr.String()
	responseString = strings.TrimRight(responseString, "\r\n")
	responseString = strings.TrimRight(responseString, "\n")

	return responseString, nil
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
