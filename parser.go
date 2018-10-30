package bloomd

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

const (
	// Valid bloomD responses
	_RESPONSE_DONE               = "Done"
	_RESPONSE_EXISTS             = "Exists"
	_RESPONSE_DELETE_IN_PROG     = "Delete in progress"
	_RESPONSE_FILTER_NOT_EXIST   = "Filter does not exist"
	_RESPONSE_FILTER_NOT_PROXIED = "Filter is not proxied. Close it first."
	_RESPONSE_YES                = "Yes"
	_RESPONSE_NO                 = "No"
)

// BloomFilter describes the bloomD filter.
type BloomFilter struct {
	Capacity    int
	Name        string
	Probability float32
	Size        int
	Storage     int // in bytes
}

// VerboseBloomFilter describes the bloomD filter in detail.
type VerboseBloomFilter struct {
	BloomFilter
	Checks      int
	CheckHits   int
	CheckMisses int
	PageIns     int
	PageOuts    int
	Sets        int
	SetHits     int
	SetMisses   int
}

// parseBool returns bloomD's reply in boolean form. If the
// response was malformed it returns an error.
func parseBool(resp string) (bool, error) {
	switch resp {
	case _RESPONSE_YES:
		return true, nil
	case _RESPONSE_NO:
		return false, nil
	default:
		return false, errors.New(resp)
	}
}

// parseBoolList returns bloomD's reply as a boolean list. If the
// response was malformed it returns an error.
func parseBoolList(n int, resp string) ([]bool, error) {
	if resp == _RESPONSE_FILTER_NOT_EXIST {
		return nil, FilterDoesNotExist
	} else if !strings.HasPrefix(resp, _RESPONSE_YES) && !strings.HasPrefix(resp, _RESPONSE_NO) {
		return nil, errors.Errorf("bloomd: unexpected response %s", resp)
	}

	results := make([]bool, 0, n)
	for _, r := range strings.Split(resp, " ") {
		results = append(results, r == _RESPONSE_YES)
	}

	return results, nil
}

// parseConfirmation returns an error if the reply was not a succesful one.
func parseConfirmation(resp string) error {
	switch resp {
	case _RESPONSE_DONE:
		return nil
	default:
		return errors.New(resp)
	}
}

// parseDropConfirmation returns an error if the reply was not a succesful one.
// It considers the response `Filter does not exist` as a non error.
func parseDropConfirmation(resp string) error {
	switch resp {
	case _RESPONSE_DONE:
		return nil
	case _RESPONSE_FILTER_NOT_EXIST:
		return nil
	default:
		return errors.New(resp)
	}
}

// parseCreate returns an error if the create reply was not a succesful one.
// It considers the response `Exists` as a non error.
func parseCreate(resp string) error {
	switch resp {
	case _RESPONSE_DONE:
		return nil
	case _RESPONSE_EXISTS:
		return nil
	// This response occurs if a filter of the same name was recently deleted, and
	// bloomd has not yet completed the delete operation.
	// TODO (eduardo): support retry
	case _RESPONSE_DELETE_IN_PROG:
		return DeleteInProgress
	default:
		return errors.New(resp)
	}
}

// parseInfo converts the response into VerboseBloomFilter.
func parseInfo(name, resp string) (VerboseBloomFilter, error) {
	lines := strings.Split(resp, "\n")

	properties := make(map[string]int)
	probability := 0.0
	for _, line := range lines {
		split := strings.SplitN(line, " ", 2)
		if len(split) != 2 {
			return VerboseBloomFilter{}, errors.Errorf("bloomd: unexpected response %s", resp)
		}

		if split[0] == "probability" {
			if v, err := strconv.ParseFloat(split[1], 32); err == nil {
				probability = v
			} else {
				return VerboseBloomFilter{}, errors.Errorf("bloomd: unexpected response %s", resp)
			}
		} else {
			if v, err := strconv.Atoi(split[1]); err == nil {
				properties[split[0]] = v
			} else {
				return VerboseBloomFilter{}, errors.Errorf("bloomd: unexpected response %s", resp)
			}
		}
	}

	bloomFilter := VerboseBloomFilter{
		BloomFilter: BloomFilter{
			Capacity:    properties["capacity"],
			Name:        name,
			Probability: float32(probability),
			Size:        properties["size"],
			Storage:     properties["storage"],
		},
		Checks:      properties["checks"],
		CheckHits:   properties["check_hits"],
		CheckMisses: properties["check_misses"],
		PageIns:     properties["page_ins"],
		PageOuts:    properties["page_outs"],
		Sets:        properties["sets"],
		SetHits:     properties["set_hits"],
		SetMisses:   properties["set_misses"],
	}

	return bloomFilter, nil
}

// parseFilterList converts the response into a list of BloomFilter.
func parseFilterList(resp string) ([]BloomFilter, error) {
	lines := strings.Split(resp, "\n")

	results := make([]BloomFilter, len(lines))
	for i, line := range lines {
		parts := strings.Split(line, " ")

		if len(parts) != 5 {
			return nil, errors.Errorf("bloomd: unexpected response %s", resp)
		}

		filter := BloomFilter{Name: parts[0]}

		if probability, err := strconv.ParseFloat(parts[1], 32); err == nil {
			filter.Probability = float32(probability)
		} else {
			return nil, errors.Errorf("bloomd: unexpected response %s", resp)
		}

		if storage, err := strconv.Atoi(parts[2]); err == nil {
			filter.Storage = storage
		} else {
			return nil, errors.Errorf("bloomd: unexpected response %s", resp)
		}

		if cap, err := strconv.Atoi(parts[3]); err == nil {
			filter.Capacity = cap
		} else {
			return nil, errors.Errorf("bloomd: unexpected response %s", resp)
		}

		if size, err := strconv.Atoi(parts[4]); err == nil {
			filter.Size = size
		} else {
			return nil, errors.Errorf("bloomd: unexpected response %s", resp)
		}

		results[i] = filter
	}

	return results, nil
}
