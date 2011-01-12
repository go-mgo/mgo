package mongogo

import (
    "sync"
)


// Synchronization primitive which enables waiting reliably for a
// condition to become true.
type cond struct {
	m                     sync.Mutex
	waiters               []*sync.Mutex
	nwaiters, popi, pushi int
}


// Wait until the provided condition function returns true.  If the
// condition function returns false, this function will block until
// another goroutine calls Signal() or Broadcast() on the same
// condition variable.  Multiple goroutines waiting on the same
// condition variable will be unblocked in FIFO order (first
// goroutine blocked will be unblocked first).
//
// If the condition function returns true, the Wait() method is
// guaranteed to unblock and return, which means it's safe to acquire
// a mutex within the condition function and return with it still
// locked. For instance:
//
//     condition.Wait(func() bool {
//         mutex.Lock()
//         if size > 0 { return true }
//         mutex.Unlock()
//         return false
//     })
//     // size is guaranteed to be > 0 here.
//     mutex.Unlock()   
//
func (c *cond) Wait(condition func() bool) {
	c.m.Lock()
	defer c.m.Unlock()
	var w sync.Mutex
    w.Lock()
	for !condition() {
		c.push(&w)
		c.m.Unlock()
		w.Lock()
		c.m.Lock()
	}
}

// Unblock the goroutine waiting on the condition variable the longest
// and allow it to retest its condition function.
func (c *cond) Signal() {
	c.m.Lock()
	w := c.pop()
	if w != nil {
		w.Unlock()
	}
	c.m.Unlock()
}

// Unblock all goroutines waiting on the condition variable and allow
// them to retest their condition function.
func (c *cond) Broadcast() {
	c.m.Lock()
	for {
		w := c.pop()
		if w == nil {
			break
		}
		w.Unlock()
	}
	c.m.Unlock()
}

// Push a waiter into the circular buffer, expanding it if necessary.
func (c *cond) push(w *sync.Mutex) {
	if c.nwaiters == len(c.waiters) {
		c.expand()
	}
	c.waiters[c.pushi] = w
	c.nwaiters++
	c.pushi = (c.pushi + 1) % len(c.waiters)
}

// Pop waiter from the circular buffer. Returns nil in case it's empty.
func (c *cond) pop() (w *sync.Mutex) {
	if c.nwaiters == 0 {
		return nil
	}
	w = c.waiters[c.popi]
	c.waiters[c.popi] = nil // Help GC.
	c.nwaiters--
	c.popi = (c.popi + 1) % len(c.waiters)
	return w
}

// Expand circular buffer capacity. This must be called when there's
// no space left.
func (c *cond) expand() {
	curcap := len(c.waiters)
	var newcap int
	if curcap == 0 {
		newcap = 8
	} else if curcap < 1024 {
		newcap = curcap * 2
	} else {
		newcap = curcap + (curcap / 4)
	}
	waiters := make([]*sync.Mutex, newcap)

	if c.popi == 0 {
		copy(waiters, c.waiters)
		c.pushi = curcap
	} else {
		newpopi := newcap - (curcap - c.popi)
		copy(waiters, c.waiters[:c.popi])
		copy(waiters[newpopi:], c.waiters[c.popi:])
		c.popi = newpopi
	}
	c.waiters = waiters
}
