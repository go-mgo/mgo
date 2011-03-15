package mongogo

import (
    "launchpad.net/gocheck"
)

type QS struct{}

var _ = gocheck.Suite(&QS{})

func (s *QS) TestSequentialGrowth(c *gocheck.C) {
    q := queue{}
    n := 2048
    for i := 0; i != n; i++ {
        q.Push(i)
    }
    for i := 0; i != n; i++ {
        c.Assert(q.Pop(), gocheck.Equals, i)
    }
}

var queueTestLists = [][]int{
    // {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},

    // {8, 9, 10, 11, ... 2, 3, 4, 5, 6, 7}
    {0, 1, 2, 3, 4, 5, 6, 7, -1, -1, 8, 9, 10, 11},

    // {8, 9, 10, 11, ... 2, 3, 4, 5, 6, 7}
    {0, 1, 2, 3, -1, -1, 4, 5, 6, 7, 8, 9, 10, 11},

    // {0, 1, 2, 3, 4, 5, 6, 7, 8}
    {0, 1, 2, 3, 4, 5, 6, 7, 8,
        -1, -1, -1, -1, -1, -1, -1, -1, -1,
        0, 1, 2, 3, 4, 5, 6, 7, 8},
}


func (s *QS) TestQueueTestLists(c *gocheck.C) {
    test := []int{}
    testi := 0
    reset := func() {
        test = test[0:0]
        testi = 0
    }
    push := func(i int) {
        test = append(test, i)
    }
    pop := func() (i int) {
        if testi == len(test) {
            return -1
        }
        i = test[testi]
        testi++
        return
    }

    for _, list := range queueTestLists {
        reset()
        q := queue{}
        for _, n := range list {
            if n == -1 {
                c.Assert(q.Pop(), gocheck.Equals, pop(),
                    gocheck.Bug("With list %#v", list))
            } else {
                q.Push(n)
                push(n)
            }
        }

        for n := pop(); n != -1; n = pop() {
            c.Assert(q.Pop(), gocheck.Equals, n,
                gocheck.Bug("With list %#v", list))
        }

        c.Assert(q.Pop(), gocheck.Equals, nil,
            gocheck.Bug("With list %#v", list))
    }
}
