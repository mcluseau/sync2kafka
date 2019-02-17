package boltindex

import (
	"math/rand"
	"time"

	"github.com/oklog/ulid"
)

var ulidEntropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)

func newUlid() ulid.ULID {
	return ulid.MustNew(ulid.Now(), ulidEntropy)
}
