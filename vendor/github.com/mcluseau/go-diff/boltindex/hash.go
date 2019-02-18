package boltindex

import (
	gohash "hash"

	"github.com/spaolacci/murmur3"
)

const (
	hashBits = 128
	hashLen  = hashBits / 8
)

type hash = murmur3.Hash128

// store the original key at 'k'+hash
func dbKeyKey(keyH gohash.Hash) []byte {
	return keyH.Sum(append(make([]byte, 0, 1+hashLen), 'k'))
}

// store the value's hash at 'v'+hash
func dbValueHashKey(keyH gohash.Hash) []byte {
	return keyH.Sum(append(make([]byte, 0, 1+hashLen), 'v'))
}

func hashOf(data []byte) hash {
	h := murmur3.New128()
	h.Write(data)
	return h
}
