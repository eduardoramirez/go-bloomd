package bloomd

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"io"
	"net"
	"strings"
	"time"
)

type Filter string

type Client interface {
	BulkSet(context.Context, Filter, ...string) ([]bool, error)
	Check(context.Context, Filter, string) (bool, error)
	Clear(context.Context, Filter) error
	Close(context.Context, Filter) error
	Create(context.Context, Filter) error
	Drop(context.Context, Filter) error
	Flush(context.Context) error
	List(context.Context) (map[string]string, error)
	MultiCheck(context.Context, Filter, ...string) ([]bool, error)
	Set(context.Context, Filter, string) (bool, error)
	Info(context.Context, Filter) (map[string]string, error)
	Ping() error
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
)

type client struct {
	hostname string
	timeout  time.Duration
	addr     *net.TCPAddr
	attempts int
	hashKeys bool
}

func NewClient(hostname string, hashKeys bool, timeout time.Duration) (Client, error) {
	// TODO probably need to have a go routine call addr to resolve DNS periodically
	addr, err := net.ResolveTCPAddr("tcp", hostname)
	if err != nil {
		return nil, err
	}

	return &client{
		hostname: hostname,
		timeout:  timeout,
		addr:     addr,
		hashKeys: hashKeys,
	}, nil
}

// Add multi keys to the filter
func (t client) BulkSet(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	return t.sendMultiCommand(BULK, name, keys...)
}

// Check if key exists in filter
func (t client) Check(ctx context.Context, name Filter, key string) (bool, error) {
	return false, errors.New("not implemented")
}

// Clears the filter
func (t client) Clear(ctx context.Context, name Filter) error {
	return t.sendCommandDone(fmt.Sprintf(DROP, name))
}

// Closes the filter
func (t client) Close(ctx context.Context, name Filter) error {
	return t.sendCommandDone(fmt.Sprintf(CLOSE, name))
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

	cmd := fmt.Sprintf(CREATE, name)
	if capacity > 0 {
		cmd = fmt.Sprintf(CREATE_CAPACITY, cmd, capacity)
	}
	if probability > 0 {
		cmd = fmt.Sprintf(CREATE_PROB, cmd, probability)
	}
	if inMemory {
		cmd = fmt.Sprintf(CREATE_INMEM, cmd)
	}

	resp, err := t.sendCommand(cmd)
	if err != nil {
		return err
	}

	switch resp {
	case RESPONSE_DONE:
		return nil

	case RESPONSE_EXISTS:
		return nil

	default:
		return errors.Errorf("Invalid create resp '%s'", resp)
	}
}

// Permanently deletes filter
func (t client) Drop(ctx context.Context, name Filter) error {
	return t.sendCommandDone(fmt.Sprintf(DROP, name))
}

// Flush to disk
func (t client) Flush(ctx context.Context) error {
	return t.sendCommandDone(FLUSH)
}

func (t client) Info(ctx context.Context, name Filter) (map[string]string, error) {
	return t.sendBlockCommand(fmt.Sprintf(INFO, name))
}

// List filters
func (t client) List(ctx context.Context) (map[string]string, error) {
	return t.sendBlockCommand(LIST)
}

// Checks whether multiple keys exist in the filter
func (t client) MultiCheck(ctx context.Context, name Filter, keys ...string) ([]bool, error) {
	return t.sendMultiCommand(MULTI, name, keys...)
}

// Add new key to filter
func (t client) Set(ctx context.Context, name Filter, key string) (bool, error) {
	cmd := fmt.Sprintf(SET, name, t.hashKey(key))
	resp, err := t.sendCommand(cmd)
	if err != nil {
		return false, err
	}

	switch resp {
	case RESPONSE_YES:
		return true, nil
	case RESPONSE_NO:
		return true, nil
	default:
		return false, errors.Errorf("Invalid response '%s'", resp)
	}
}

func (t client) Ping() error {
	return errors.New("not implemented")
}

// Returns the key we should send to the server
func (t client) hashKey(key string) string {
	if t.hashKeys {
		h := sha1.New()
		s := h.Sum([]byte(key))
		return fmt.Sprintf("%x", s)
	}
	return key
}

func (t client) sendCommandDone(cmd string) error {
	resp, err := t.sendCommand(cmd)
	if err != nil {
		return errors.Wrapf(err, "bloomd: error with command '%s'", cmd)
	}

	if resp != RESPONSE_DONE {
		return errors.Errorf("bloomd: error with resp '%s' command '%s'", resp, cmd)
	}

	return nil
}

func (t client) sendMultiCommand(c string, name Filter, keys ...string) ([]bool, error) {
	cmd := t.multiCommand(c, name, keys...)

	resp, err := t.sendCommand(cmd)
	if err != nil {
		return nil, errors.Wrapf(err, "bloomd: error with multi command '%s'", cmd)
	}

	if !strings.HasPrefix(resp, RESPONSE_YES) && !strings.HasPrefix(resp, RESPONSE_NO) {
		return nil, errors.Errorf("bloomd: error with multi response '%s' command '%s'", resp, cmd)
	}

	results := make([]bool, 0, len(keys))
	responses := strings.Split(resp, " ")
	for _, r := range responses {
		results = append(results, r == RESPONSE_YES)
	}

	return results, nil
}

func (t client) multiCommand(cmd string, name Filter, keys ...string) string {
	buf := &bytes.Buffer{}
	buf.WriteString(cmd)
	buf.WriteRune(' ')
	buf.WriteString(string(name))
	for _, key := range keys {
		buf.WriteRune(' ')
		buf.WriteString(t.hashKey(key))
	}
	return buf.String()
}

func (t client) sendBlockCommand(cmd string) (map[string]string, error) {
	resp, err := t.sendCommand(LIST)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(resp, "START") {
		return nil, errors.Errorf("bloomd: invalid list start '%s' command '%s'", resp, cmd)
	}

	responses := make(map[string]string)

	split := strings.Split(resp, "\n")
	for _, line := range split {
		line := strings.TrimSpace(line)

		switch line {
		case "START":
		case "END":
		case "":
		default:
			split := strings.SplitN(line, " ", 2)
			responses[split[0]] = split[1]
		}
	}
	return responses, nil
}

func (t client) sendCommand(cmd string) (string, error) {

	conn, err := newConnection(t.addr, t.attempts)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if err := send(conn, cmd, t.attempts); err != nil {
		return "", err
	}

	line, err := recv(conn)
	if err != nil {
		return "", err
	}

	return line, nil
}

func newConnection(addr *net.TCPAddr, attempts int) (io.ReadWriteCloser, error) {
	attempted := 0

	var conn net.Conn
	var err error
	for {
		conn, err = net.DialTCP("tcp", nil, addr)
		if err != nil && attempted > attempts {
			return nil, errors.Wrap(err, "bloomd: unable to establish a connection")
		}
		attempted++
	}

	return conn, nil
}

func send(w io.Writer, cmd string, attempts int) error {
	attempted := 0

	for {
		_, err := w.Write([]byte(cmd + "\n"))
		if err != nil && attempted > attempts {
			return errors.Wrap(err, "bloomd: unable to write to connection")
		}
		attempted++
	}

	return nil
}

func recv(r io.Reader) (string, error) {
	//TODO read block
	line, err := bufio.NewReader(r).ReadString('\n')
	if err != nil && err != io.EOF {
		return line, errors.Wrap(err, "bloomd: unable to read connection")
	}
	return strings.TrimRight(line, "\r\n"), nil
}
