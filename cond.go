package mongogo

import (
    "sync"
)


type cond struct {
    m sync.Mutex
    waiters []*sync.Mutex
    nwaiters, popi, pushi int
}

func (c *cond) Wait(condition func() bool) {
    c.m.Lock()
    defer c.m.Unlock()
    var w sync.Mutex
    for !condition() {
        w.Lock()
        c.push(&w)
        c.m.Unlock()
        w.Lock()
        c.m.Lock()
    }
}

func (c *cond) Broadcast() {
    c.m.Lock()
    for {
        w := c.pop()
        if w == nil { break }
        w.Unlock()
    }
    c.m.Unlock()
}

func (c *cond) Signal() {
    c.m.Lock()
    w := c.pop()
    if w != nil {
        w.Unlock()
    }
    c.m.Unlock()
}

func (c *cond) push(w *sync.Mutex) {
    if c.nwaiters == len(c.waiters) {
        c.expand()
    }
    c.waiters[c.pushi] = w
    c.nwaiters++
    c.pushi = (c.pushi + 1) % len(c.waiters)
}

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

func (c *cond) expand() {
    curcap := len(c.waiters)
    var newcap int
    if curcap == 0 {
        newcap = 8
    } else if curcap < 1024 {
        newcap = curcap * 2
    } else {
        newcap = (curcap * 2) / 4
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
