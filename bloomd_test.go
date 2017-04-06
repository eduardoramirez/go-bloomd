package bloomd

import (
	"flag"
	"fmt"
	"golang.org/x/net/context"
	. "gopkg.in/check.v1"
	"sync"
	"testing"
	"time"
)

const (
	BLOOMD_HOST = "localhost:8673"

	TEST_FILTER = "test_filter"
)

var (
	hostname      string
	multiThread   bool
	numWorkers    int
	numIterations int
)

func init() {
	flag.StringVar(&hostname, "host", BLOOMD_HOST, "Bloomd Test Host")
	flag.BoolVar(&multiThread, "test-multi-thread", false, "Enable multi-threaded tests")
	flag.IntVar(&numWorkers, "multi-thread-threads", 4, "Number of Workers")
	flag.IntVar(&numIterations, "multi-thread-iterations", 1000, "Number of Iterations")
}

func Test(t *testing.T) {
	TestingT(t)
}

type BloomdSuite struct {
	client Client
}

var _ = Suite(&BloomdSuite{})

func (s *BloomdSuite) SetUpSuite(c *C) {
	flag.Parse()

	client, err := NewClient(hostname, false, time.Second)
	c.Assert(err, IsNil)
	c.Assert(client.Ping(), IsNil)

	s.client = client
}

func (s *BloomdSuite) TestList(c *C) {
	ctx := context.Background()

	m, err := s.client.List(ctx)
	c.Assert(err, IsNil)

	for k, v := range m {
		fmt.Println(k, v)
	}
}

func (s *BloomdSuite) TestGetSetGet(c *C) {
	ctx := context.Background()

	c.Assert(s.client.Create(ctx, TEST_FILTER), IsNil)

	keys := []string{
		"test-1",
		"test-2",
		"test-3",
		"test-4",
		"test-5",
	}

	r1, err := s.client.MultiCheck(ctx, TEST_FILTER, keys...)
	c.Assert(err, IsNil)
	for i := 0; i < len(r1); i++ {
		c.Assert(r1[i], Equals, false)
	}

	r2, err := s.client.MultiSet(ctx, TEST_FILTER, "test-1", "test-3", "test-5")
	c.Assert(err, IsNil)
	for i := 0; i < len(r2); i++ {
		c.Assert(r2[i], Equals, true)
	}

	r3, err := s.client.MultiCheck(ctx, TEST_FILTER, keys...)
	c.Assert(err, IsNil)
	for i := 0; i < len(r3); i++ {
		c.Assert(r3[i], Equals, i%2 == 0)
	}

	c.Assert(s.client.Drop(ctx, TEST_FILTER), IsNil)
}

func (s *BloomdSuite) TestMultiThread(c *C) {
	s.testMultiThread(c, s.client)
}

func (s *BloomdSuite) TestPooledMultiThread(c *C) {
	pooledClient, err := NewPooledClient(hostname, false, time.Second, 5, 10)
	c.Assert(err, IsNil)
	s.testMultiThread(c, pooledClient)
}

func (s *BloomdSuite) testMultiThread(c *C, client Client) {
	ctx := context.Background()
	c.Assert(s.client.Create(ctx, TEST_FILTER+"_MULTI"), IsNil)

	cw := make(chan int, numWorkers)
	ce := make(chan error, numIterations)

	wg := &sync.WaitGroup{}
	for j := 0; j < numWorkers; j++ {
		wg.Add(1)
		go hitBloomd(s.client, wg, cw, ce)
	}

	for i := 0; i < numWorkers; i++ {
		cw <- i
	}
	close(cw)

	wg.Wait()
	close(ce)

	// Handle errors
	for err := range ce {
		c.Assert(err, IsNil)
	}
}

func hitBloomd(client Client, wg *sync.WaitGroup, c chan int, ce chan error) {
	defer wg.Done()

	ctx := context.Background()
	for _ = range c {
		_, err := client.MultiCheck(ctx, TEST_FILTER+"_MULTI", "test")
		if err != nil {
			ce <- err
		}
	}
}
