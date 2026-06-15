package Http

import (
	"time"
)

// Timeout - timeouts for api calls
// default value is 1s
type Timeout time.Duration

const (
	TimeoutDefault Timeout = iota
	Timeout100ms
	Timeout200ms
	Timeout500ms
	Timeout1s
	Timeout2s
	Timeout5s
	Timeout10s
)

func (t Timeout) GetDuration() time.Duration {
	switch t {
	case Timeout100ms:
		return time.Millisecond * 100
	case Timeout200ms:
		return time.Millisecond * 200
	case Timeout500ms:
		return time.Millisecond * 500
	case Timeout1s, TimeoutDefault:
		return time.Second * 1
	case Timeout2s:
		return time.Second * 2
	case Timeout5s:
		return time.Second * 5
	case Timeout10s:
		return time.Second * 10
	}
	return time.Duration(t)
}
