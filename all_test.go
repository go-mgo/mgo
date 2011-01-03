package mongogo_test


import (
    . "gocheck"
    B "gobson"
    "mongogo"
)


// Connect to the master of a deployment with a single server,
// run an insert, and then ensure the insert worked and that a
// single connection was established.
func (s *S) TestTopologySyncWithSingleMaster(c *C) {
    // Use hostname here rather than IP, to make things trickier.
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(B.M{"a": 1, "b": 2})

    result := struct{Ok bool}{}
    err = session.Run("getLastError", &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, true)

    // One connection during discovery. Master socket
    // recycled for insert. 
    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 0)
}

func (s *S) TestTopologySyncWithSlaveSeed(c *C) {
    // That's supposed to be a slave. Must run discovery
    // and find out master to insert successfully.
    session, err := mongogo.Mongo("localhost:40012")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(B.M{"a": 1, "b": 2})

    result := struct{Ok bool}{}
    err = session.Run("getLastError", &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, true)

    // One connection to each during discovery. Master
    // socket recycled for insert. 
    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 2)
}

func (s *S) TestRunString(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    result := struct{Ok int}{}
    err = session.Run("ping", &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestRunValue(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    result := struct{Ok int}{}
    err = session.Run(B.M{"ping": 1}, &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestInsertFindOne(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(B.M{"a": 1, "b": 2})

    result := struct{A, B int}{}

    err = coll.Find(B.M{"a": 1}).One(&result)
    c.Assert(err, IsNil)
    c.Assert(result.A, Equals, 1)
    c.Assert(result.B, Equals, 2)
}

func (s *S) TestInsertFindOneM(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(B.M{"a": 1, "b": 2})
    result := make(B.M)
    err = coll.Find(B.M{"a": 1}).One(result)
    c.Assert(err, IsNil)
    c.Assert(result["a"], Equals, int32(1))
    c.Assert(result["b"], Equals, int32(2))
}

func (s *S) TestInsertFindOneNotFound(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")

    result := struct{A, B int}{}
    err = coll.Find(B.M{"a": 1}).One(&result)
    c.Assert(err, Equals, mongogo.NotFound)
    c.Assert(err, Matches, "Document not found")
    c.Assert(err == mongogo.NotFound, Equals, true)
}

func (s *S) TestInsertFindIter(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")

    ns := []int{40, 41, 42, 43, 44, 45, 46}
    for _, n := range ns {
        coll.Insert(B.M{"n": n})
    }

    mongogo.ResetStats()

    query := coll.Find(B.M{"n": B.M{"$gte": 42}}).Prefetch(0).Batch(2)
    iter, err := query.Iter()
    c.Assert(err, IsNil)

    // Results may be unordered. We need a map.
    m := make(map[int]bool)
    for _, n := range ns[2:] {
        m[n] = true
    }

    n := len(m)
    result := struct{N int}{}
    for i := 0; i != n; i++ {
        err = iter.Next(&result)
        c.Assert(err, IsNil)
        if _, ok := m[result.N]; !ok {
            c.Fatalf("Find returned document with unexpected n=%d", result.N)
        } else {
            c.Log("Popping document with n=", result.N)
            m[result.N] = false, false
        }

        if i == 1 { // The batch size.
            stats := mongogo.GetStats()
            c.Assert(stats.ReceivedDocs, Equals, 2)
        }

    }

    for n, _ := range m {
        c.Fatalf("Find didn't return document with n=%d", n)
    }

    err = iter.Next(&result)
    c.Assert(err == mongogo.NotFound, Equals, true)

    stats := mongogo.GetStats()
    c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 2*GET_MORE_OP
    c.Assert(stats.ReceivedOps, Equals, 3) // and their REPLY_OPs.
    c.Assert(stats.ReceivedDocs, Equals, 5)
}

func (s *S) TestSort(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")

    coll.Insert(B.M{"a": 1, "b": 1})
    coll.Insert(B.M{"a": 2, "b": 2})
    coll.Insert(B.M{"a": 2, "b": 1})
    coll.Insert(B.M{"a": 0, "b": 1})
    coll.Insert(B.M{"a": 2, "b": 0})
    coll.Insert(B.M{"a": 0, "b": 2})
    coll.Insert(B.M{"a": 1, "b": 2})
    coll.Insert(B.M{"a": 0, "b": 0})
    coll.Insert(B.M{"a": 1, "b": 0})

    query := coll.Find(B.M{})
    query.Sort(B.D{{"a", -1}}) // Should be ignored.
    iter, err := query.Sort(B.D{{"b", -1}, {"a", 1}}).Iter()
    c.Assert(err, IsNil)

    l := make([]int, 18)
    r := struct{A, B int}{}
    for i := 0; i != len(l); i += 2 {
        err := iter.Next(&r)
        c.Assert(err, IsNil)
        l[i] = r.A
        l[i+1] = r.B
    }

    c.Assert(l, Equals,
             []int{0, 2, 1, 2, 2, 2, 0, 1, 1, 1, 2, 1, 0, 0, 1, 0, 2, 0})
}

func (s *S) TestBadSortWithMap(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    coll := session.DB("mydb").C("mycollection")

    defer func() {
        error := recover()
        c.Assert(error, Matches,
                 "Can't sort using map of len > 1. Use gobson.D instead.")
    }()
    coll.Find(B.M{}).Sort(B.M{"a": 1, "b": 2})
}

func (s *S) TestInsertFindIterTwiceWithSameQuery(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")

    for i := 40; i != 47; i++ {
        coll.Insert(B.M{"n": i})
    }

    query := coll.Find(B.M{}).Sort(B.M{"n": 1})

    result1, err := query.Skip(1).Iter()
    c.Assert(err, IsNil)
    result2, err := query.Skip(2).Iter()
    c.Assert(err, IsNil)

    result := struct{N int}{}
    err = result2.Next(&result)
    c.Assert(err, IsNil)
    c.Assert(result.N, Equals, 42)
    err = result1.Next(&result)
    c.Assert(err, IsNil)
    c.Assert(result.N, Equals, 41)
}

func (s *S) TestInsertFindIterWithoutResults(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(B.M{"n": 42})

    iter, err := coll.Find(B.M{"n": 0}).Iter()
    c.Assert(err, IsNil)

    result := struct{N int}{}
    err = iter.Next(&result)
    c.Assert(result.N, Equals, 0)
    c.Assert(err == mongogo.NotFound, Equals, true)
}

func (s *S) TestPrefetching(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")

    docs := make([]interface{}, 200)
    for i := 0; i != 200; i++ {
        docs[i] = B.M{"n": i}
    }
    coll.Insert(docs...)

    // Same test three times.  Once with prefetching via query, then with the
    // default prefetching, and a third time tweaking the default settings in
    // the session.
    for testi := 0; testi != 3; testi++ {
        mongogo.ResetStats()

        var iter *mongogo.Iter
        var nextn int

        switch testi {
        case 0: // First, using query methods.
            iter, err = coll.Find(B.M{}).Prefetch(0.27).Batch(100).Iter()
            c.Assert(err, IsNil)
            nextn = 73

        case 1: // Then, the default session value.
            session.Batch(100)
            iter, err = coll.Find(B.M{}).Iter()
            c.Assert(err, IsNil)
            nextn = 75

        case 2: // Then, tweaking the session value.
            session.Batch(100)
            session.Prefetch(0.27)
            iter, err = coll.Find(B.M{}).Iter()
            c.Assert(err, IsNil)
            nextn = 73
        }

        result := struct{N int}{}
        for i := 0; i != nextn; i++ {
            iter.Next(&result)
        }

        stats := mongogo.GetStats()
        c.Assert(stats.ReceivedDocs, Equals, 100)

        iter.Next(&result)

        // Ping the database just to wait for the fetch above
        // to get delivered.
        session.Run("ping", B.M{}) // XXX Should support nil here.

        stats = mongogo.GetStats()
        c.Assert(stats.ReceivedDocs, Equals, 201) // 200 + the ping result
    }
}
