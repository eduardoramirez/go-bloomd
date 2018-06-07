package bloomd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseBool(t *testing.T) {
	assert := assert.New(t)

	res, err := parseBool("Yes")
	assert.NoError(err)
	assert.Equal(true, res)

	res, err = parseBool("No")
	assert.NoError(err)
	assert.Equal(false, res)

	res, err = parseBool("Wrong answer")
	assert.Error(err)
	assert.Equal(false, res)
}

func TestParseBoolList(t *testing.T) {
	assert := assert.New(t)

	res, err := parseBoolList(2, "Yes Yes")
	assert.NoError(err)
	assert.Equal([]bool{true, true}, res)

	res, err = parseBoolList(1, "No No")
	assert.NoError(err)
	assert.Equal([]bool{false, false}, res)

	res, err = parseBoolList(4, "Yes Yes Yes Yes")
	assert.NoError(err)
	assert.Equal([]bool{true, true, true, true}, res)

	res, err = parseBoolList(4, "No No No No")
	assert.NoError(err)
	assert.Equal([]bool{false, false, false, false}, res)

	res, err = parseBoolList(4, "No Yes No Yes")
	assert.NoError(err)
	assert.Equal([]bool{false, true, false, true}, res)
}

func TestParseConfirmation(t *testing.T) {
	assert := assert.New(t)

	err := parseConfirmation("Done")
	assert.NoError(err)

	err = parseConfirmation("Filter does not exist")
	assert.Error(err)

	err = parseConfirmation("Filter is not proxied. Close it first.")
	assert.Error(err)

	err = parseConfirmation("Wrong val")
	assert.Error(err)
}

func TestParseDropConfirmation(t *testing.T) {
	assert := assert.New(t)

	err := parseDropConfirmation("Done")
	assert.NoError(err)

	err = parseDropConfirmation("Filter does not exist")
	assert.NoError(err)

	err = parseDropConfirmation("Filter is not proxied. Close it first.")
	assert.Error(err)

	err = parseDropConfirmation("Wrong val")
	assert.Error(err)
}

func TestParseCreate(t *testing.T) {
	assert := assert.New(t)

	err := parseCreate("Done")
	assert.NoError(err)

	err = parseCreate("Exists")
	assert.NoError(err)

	err = parseCreate("Delete in progress")
	assert.Error(err)

	err = parseCreate("Wrong val")
	assert.Error(err)
}

func TestParseInfo(t *testing.T) {
	assert := assert.New(t)

	res := "capacity 1000000\nchecks 3\ncheck_hits 2\ncheck_misses 1\npage_ins 3\npage_outs 3\nprobability 0.001\nsets 4\nset_hits 3\nset_misses 1\nsize 4\nstorage 1797211"
	filter, err := parseInfo("Filtero", res)
	assert.NotNil(filter)
	assert.NoError(err)

	assert.Equal("Filtero", filter.Name)
	assert.Equal(1000000, filter.Capacity)
	assert.Equal(3, filter.Checks)
	assert.Equal(2, filter.CheckHits)
	assert.Equal(1, filter.CheckMisses)
	assert.Equal(3, filter.PageIns)
	assert.Equal(3, filter.PageOuts)
	assert.Equal(float32(0.001), filter.Probability)
	assert.Equal(4, filter.Sets)
	assert.Equal(3, filter.SetHits)
	assert.Equal(1, filter.SetMisses)
	assert.Equal(4, filter.Size)
	assert.Equal(1797211, filter.Storage)

	filter, err = parseInfo("Filtero", "garbage")
	assert.Error(err)
}

func TestParseFilterList(t *testing.T) {
	assert := assert.New(t)

	res := "bar 0.000100 300046 100000 1\nfoo 0.000200 300056 200000 2\ncar 0.000300 300066 300000 3"
	filters, err := parseFilterList(res)
	assert.NotNil(filters)
	assert.NoError(err)

	expected := []BloomFilter{
		{Name: "bar", Probability: float32(0.000100), Storage: 300046, Capacity: 100000, Size: 1},
		{Name: "foo", Probability: float32(0.000200), Storage: 300056, Capacity: 200000, Size: 2},
		{Name: "car", Probability: float32(0.000300), Storage: 300066, Capacity: 300000, Size: 3},
	}

	assert.Equal(expected, filters)
}
