package mongogo

import (
    "sync"
)


type cond struct {
    m sync.Mutex
    wait []*sync.Mutex
    popi, pushi int
    waiters int
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
    if c.waiters == len(c.wait) {
        c.expand()
    }
    c.wait[c.pushi] = w
    c.waiters++
    c.pushi = (c.pushi + 1) % len(c.wait)
}

func (c *cond) pop() (w *sync.Mutex) {
    if c.waiters == 0 {
        return nil
    }
    w = c.wait[c.popi]
    c.wait[c.popi] = nil // Help GC.
    c.waiters--
    c.popi = (c.popi + 1) % len(c.wait)
    return w
}

func (c *cond) expand() {
    // Allocate new slice.
    curcap := len(c.wait)
    var newcap int
    if curcap == 0 {
        newcap = 8
    } else if curcap < 1024 {
        newcap = curcap * 2
    } else {
        newcap = (curcap * 2) / 4
    }
    wait := make([]*sync.Mutex, newcap)

    // Move old data to the new slice.
    newpopi := (newcap - (curcap - c.popi)) % newcap
    if c.popi == 0 {
        copy(wait, c.wait)
    } else {
        copy(wait, c.wait[:c.popi])
        copy(wait[newpopi:], c.wait[c.popi:])
    }

    // Use new data.
    c.popi = newpopi
    c.wait = wait
}
