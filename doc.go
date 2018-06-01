// A thread safe BloomD client. (https://github.com/armon/bloomd)
//
// Requires Golang 10 or above.
//
// Each request to bloomD will use one of the connections from the pool. A connection
// is one to one with the request and thus is thread safe. Once the request is complete
// the connection will be released back to the pool. There are a few configurations that
// can be applied to the client, refer to `Option`.
//
// More info about bloom filters: http://en.wikipedia.org/wiki/Bloom_filter
package bloomd
