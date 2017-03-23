package bloomd

import (
	"flag"
	"fmt"
	"golang.org/x/net/context"
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

const (
	BLOOMD_HOST = "localhost:8673"

	TEST_FILTER = "test_filter"
)

var hostname string

func init() {
	flag.StringVar(&hostname, "host", BLOOMD_HOST, "Bloomd Test Host")
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
