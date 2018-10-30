package bloomd

import "errors"

var (
	FilterDoesNotExist = errors.New("Filter does not exist")
	DeleteInProgress   = errors.New("Delete in progress")
)
