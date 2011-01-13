package mongogo


import (
    "runtime"
    "sync"
)


type Locker interface {
    Lock()
    Unlock()
}

type rlocker sync.RWMutex

func (r *rlocker) Lock()   { (*sync.RWMutex)(r).RLock() }
func (r *rlocker) Unlock() { (*sync.RWMutex)(r).RUnlock() }

// Cond enables waiting reliably for an arbitrary condition to become
// true. The M Locker object must be provided and will be unlocked and
// locked on calls to Wait().
type cond struct {
    M    Locker
    m    sync.Mutex
    n    uint32
    sema *uint32
}

// NewCond returns a new Cond variable with M initialized to m.  The Mutex and
// RWMutex values implement the Locker interface and are thus suitable as an
// argument.
func newCond(m Locker) *cond {
    return &cond{M: m}
}

// Wait blocks the calling goroutine and waits until it is awaken by
// a call to either Signal or Broadcast on the condition variable. It is
// a runtime error to call Wait without holding the lock on M by calling
// c.M.Lock() on it first.  M will be unlocked before sleeping, and will
// be reacquired before Wait returns.
func (c *cond) Wait() {
    c.m.Lock()
    if c.sema == nil {
        c.sema = new(uint32)
    }
    s := c.sema
    c.n++
    c.m.Unlock()
    c.M.Unlock()
    runtime.Semacquire(s)
    c.M.Lock()
}

// Signal awakes one goroutine currently waiting on the condition variable.
// There is no guarantee about which goroutine will be awaken first.
func (c *cond) Signal() {
    c.m.Lock()
    if c.n > 0 {
        c.n--
        runtime.Semrelease(c.sema)
    }
    c.m.Unlock()
}

// Broadcast awakes all goroutines currently waiting on the condition
// variable.
func (c *cond) Broadcast() {
    c.m.Lock()
    n := c.n
    if n > 0 {
        s := c.sema
        for i := uint32(0); i < n; i++ {
            runtime.Semrelease(s)
        }
        c.sema = nil // Prevent races.
        c.n = 0
    }
    c.m.Unlock()
}
