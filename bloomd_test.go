package bloomd

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// NOTE: Requires bloomd to be available as `bloomd`

const (
	testBloomdHost = "localhost:8673"
	testFilter1    = "test_filter_1"
	testFilter2    = "test_filter_2"
)

var bloomd *exec.Cmd

func TestNewClient(t *testing.T) {
	assert := assert.New(t)
	startBloomdServer()
	defer killBloomdServer()
	client, err := NewClient(testBloomdHost)
	assert.NoError(err)
	assert.NoError(client.Ping())
}

func TestListFilters(t *testing.T) {
	assert := assert.New(t)
	startBloomdServer()
	defer killBloomdServer()

	client, _ := NewClient(testBloomdHost)

	ctx := context.Background()

	assert.NoError(client.Create(ctx, testFilter1))
	assert.NoError(client.Create(ctx, testFilter2))

	m, err := client.List(ctx)
	assert.NoError(err)

	assert.Equal(2, len(m))

	_, ok := m[testFilter1]
	assert.Equal(true, ok)
	_, ok = m[testFilter2]
	assert.Equal(true, ok)

	assert.NoError(client.Drop(ctx, testFilter1))
	assert.NoError(client.Drop(ctx, testFilter2))
}

func TestGetSetGet(t *testing.T) {
	assert := assert.New(t)
	startBloomdServer()
	defer killBloomdServer()

	client, _ := NewClient(testBloomdHost)
	ctx := context.Background()

	assert.NoError(client.Create(ctx, testFilter1))

	keys := []string{
		"test-1",
		"test-2",
		"test-3",
		"test-4",
		"test-5",
	}

	r1, err := client.Multi(ctx, testFilter1, keys...)
	assert.NoError(err)
	for i := 0; i < len(r1); i++ {
		assert.Equal(false, r1[i])
	}

	r2, err := client.Bulk(ctx, testFilter1, "test-1", "test-3", "test-5")
	assert.NoError(err)
	for i := 0; i < len(r2); i++ {
		assert.Equal(true, r2[i])
	}

	r3, err := client.Multi(ctx, testFilter1, keys...)
	assert.NoError(err)
	for i := 0; i < len(r3); i++ {
		assert.Equal(i%2 == 0, r3[i])
	}

	assert.NoError(client.Drop(ctx, testFilter1))
}

func startBloomdServer() {
	bloomd = exec.Command("bloomd")
	bloomd.Start()
	time.Sleep(time.Millisecond * 10)
}

func killBloomdServer() {
	bloomd.Process.Kill()
	time.Sleep(time.Millisecond * 10)
}
