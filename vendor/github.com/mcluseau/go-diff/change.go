package diff

import (
	"fmt"
)

type ChangeType int

const (
	Unchanged = iota
	Created
	Modified
	Deleted
)

type Change struct {
	Type ChangeType
	Key  []byte
	// The new value (Created and Modified events only)
	Value []byte
}

func (c Change) String() string {
	var s string
	switch c.Type {
	case Unchanged:
		s = "U"
	case Created:
		s = "C"
	case Modified:
		s = "M"
	case Deleted:
		s = "D"
	default:
		panic(fmt.Errorf("Invalid change type: %d", c.Type))
	}

	return fmt.Sprintf("%s %q %q", s, c.Key, c.Value)
}
