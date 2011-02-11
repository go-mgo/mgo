package mongogo_test

import (
    "flag"
    "gobson"
    .   "gocheck"
    "mongogo"
    "os"
    "strings"
    "time"
)

var fast = flag.Bool("fast", false, "Skip slow tests")

type M gobson.M

// Connect to the master of a deployment with a single server,
// run an insert, and then ensure the insert worked and that a
// single connection was established.
func (s *S) TestTopologySyncWithSingleMaster(c *C) {
    // Use hostname here rather than IP, to make things trickier.
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"a": 1, "b": 2})
    c.Assert(err, IsNil)

    // One connection used for discovery. Master socket recycled for
    // insert. Socket is reserved after insert.
    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 0)
    c.Assert(stats.SocketsInUse, Equals, 1)

    // Restart session and socket must be released.
    session.Restart()
    stats = mongogo.GetStats()
    c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestTopologySyncWithSlaveSeed(c *C) {
    // That's supposed to be a slave. Must run discovery
    // and find out master to insert successfully.
    session, err := mongogo.Mongo("localhost:40012")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(M{"a": 1, "b": 2})

    result := struct{ Ok bool }{}
    err = session.Run("getLastError", &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, true)

    // One connection to each during discovery. Master
    // socket recycled for insert. 
    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 2)

    // Only one socket reference alive, in the master socket owned
    // by the above session.
    c.Assert(stats.SocketsInUse, Equals, 1)

    // Restart it, and it must be gone.
    session.Restart()
    stats = mongogo.GetStats()
    c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestRunString(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    result := struct{ Ok int }{}
    err = session.Run("ping", &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestRunValue(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    result := struct{ Ok int }{}
    err = session.Run(M{"ping": 1}, &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestInsertFindOne(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(M{"a": 1, "b": 2})

    result := struct{ A, B int }{}

    err = coll.Find(M{"a": 1}).One(&result)
    c.Assert(err, IsNil)
    c.Assert(result.A, Equals, 1)
    c.Assert(result.B, Equals, 2)
}

func (s *S) TestInsertFindOneMap(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(M{"a": 1, "b": 2})
    result := make(M)
    err = coll.Find(M{"a": 1}).One(result)
    c.Assert(err, IsNil)
    c.Assert(result["a"], Equals, int32(1))
    c.Assert(result["b"], Equals, int32(2))
}

func (s *S) TestInsertFindOneNotFound(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")

    result := struct{ A, B int }{}
    err = coll.Find(M{"a": 1}).One(&result)
    c.Assert(err, Equals, mongogo.NotFound)
    c.Assert(err, Matches, "Document not found")
    c.Assert(err == mongogo.NotFound, Equals, true)
}

func (s *S) TestInsertFindIter(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")

    ns := []int{40, 41, 42, 43, 44, 45, 46}
    for _, n := range ns {
        coll.Insert(M{"n": n})
    }

    session.Restart() // Release socket.

    mongogo.ResetStats()

    query := coll.Find(M{"n": M{"$gte": 42}}).Prefetch(0).Batch(2)
    iter, err := query.Iter()
    c.Assert(err, IsNil)

    // Results may be unordered. We need a map.
    m := make(map[int]bool)
    for _, n := range ns[2:] {
        m[n] = true
    }

    n := len(m)
    result := struct{ N int }{}
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

    session.Restart() // Release socket.

    stats := mongogo.GetStats()
    c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 2*GET_MORE_OP
    c.Assert(stats.ReceivedOps, Equals, 3) // and their REPLY_OPs.
    c.Assert(stats.ReceivedDocs, Equals, 5)
    c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestSort(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")

    coll.Insert(M{"a": 1, "b": 1})
    coll.Insert(M{"a": 2, "b": 2})
    coll.Insert(M{"a": 2, "b": 1})
    coll.Insert(M{"a": 0, "b": 1})
    coll.Insert(M{"a": 2, "b": 0})
    coll.Insert(M{"a": 0, "b": 2})
    coll.Insert(M{"a": 1, "b": 2})
    coll.Insert(M{"a": 0, "b": 0})
    coll.Insert(M{"a": 1, "b": 0})

    query := coll.Find(M{})
    query.Sort(gobson.D{{"a", -1}}) // Should be ignored.
    iter, err := query.Sort(gobson.D{{"b", -1}, {"a", 1}}).Iter()
    c.Assert(err, IsNil)

    l := make([]int, 18)
    r := struct{ A, B int }{}
    for i := 0; i != len(l); i += 2 {
        err := iter.Next(&r)
        c.Assert(err, IsNil)
        l[i] = r.A
        l[i+1] = r.B
    }

    c.Assert(l, Equals,
        []int{0, 2, 1, 2, 2, 2, 0, 1, 1, 1, 2, 1, 0, 0, 1, 0, 2, 0})
}

func (s *S) TestInsertFindIterTwiceWithSameQuery(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")

    for i := 40; i != 47; i++ {
        coll.Insert(M{"n": i})
    }

    query := coll.Find(M{}).Sort(M{"n": 1})

    result1, err := query.Skip(1).Iter()
    c.Assert(err, IsNil)
    result2, err := query.Skip(2).Iter()
    c.Assert(err, IsNil)

    result := struct{ N int }{}
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
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(M{"n": 42})

    iter, err := coll.Find(M{"n": 0}).Iter()
    c.Assert(err, IsNil)

    result := struct{ N int }{}
    err = iter.Next(&result)
    c.Assert(result.N, Equals, 0)
    c.Assert(err == mongogo.NotFound, Equals, true)
}

func (s *S) TestPrefetching(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")

    docs := make([]interface{}, 200)
    for i := 0; i != 200; i++ {
        docs[i] = M{"n": i}
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
            iter, err = coll.Find(M{}).Prefetch(0.27).Batch(100).Iter()
            c.Assert(err, IsNil)
            nextn = 73

        case 1: // Then, the default session value.
            session.Batch(100)
            iter, err = coll.Find(M{}).Iter()
            c.Assert(err, IsNil)
            nextn = 75

        case 2: // Then, tweaking the session value.
            session.Batch(100)
            session.Prefetch(0.27)
            iter, err = coll.Find(M{}).Iter()
            c.Assert(err, IsNil)
            nextn = 73
        }

        result := struct{ N int }{}
        for i := 0; i != nextn; i++ {
            iter.Next(&result)
        }

        stats := mongogo.GetStats()
        c.Assert(stats.ReceivedDocs, Equals, 100)

        iter.Next(&result)

        // Ping the database just to wait for the fetch above
        // to get delivered.
        session.Run("ping", M{}) // XXX Should support nil here.

        stats = mongogo.GetStats()
        c.Assert(stats.ReceivedDocs, Equals, 201) // 200 + the ping result
    }
}

func (s *S) TestSafeInsert(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")

    // Insert an element with a predefined key.
    err = coll.Insert(M{"_id": 1})
    c.Assert(err, IsNil)

    mongogo.ResetStats()

    // Session should be safe by default, so inserting it again must fail.
    err = coll.Insert(M{"_id": 1})
    c.Assert(err, Matches, "E11000 duplicate.*")
    c.Assert(err.(*mongogo.LastError).Code, Equals, 11000)

    // It must have sent two operations (INSERT_OP + getLastError QUERY_OP)
    stats := mongogo.GetStats()
    c.Assert(stats.SentOps, Equals, 2)

    mongogo.ResetStats()

    // If we disable safety, though, it won't complain.
    session.Unsafe()
    err = coll.Insert(M{"_id": 1})
    c.Assert(err, IsNil)

    // Must have sent a single operation this time (just the INSERT_OP)
    stats = mongogo.GetStats()
    c.Assert(stats.SentOps, Equals, 1)
}


func (s *S) TestSafeParameters(c *C) {
    session, err := mongogo.Mongo("localhost:40011")
    c.Assert(err, IsNil)
    defer session.Close()

    coll := session.DB("mydb").C("mycollection")

    // Tweak the safety parameters to something unachievable.
    session.Safe(4, 100, false)
    err = coll.Insert(M{"_id": 1})
    c.Assert(err, Matches, "timeout")
    c.Assert(err.(*mongogo.LastError).WTimeout, Equals, true)
}

func (s *S) TestNewSession(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    // Do a dummy operation to wait for connection.
    coll := session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"_id": 1})
    c.Assert(err, IsNil)

    // Tweak safety and query settings to ensure other has copied those.
    session.Unsafe()
    session.Batch(-1)
    other := session.New()
    defer other.Close()
    session.Safe(0, 0, false)

    // Clone was copied while session was unsafe, so no errors.
    otherColl := other.DB("mydb").C("mycollection")
    err = otherColl.Insert(M{"_id": 1})
    c.Assert(err, IsNil)

    // Original session was made safe again.
    err = coll.Insert(M{"_id": 1})
    c.Assert(err, NotNil)

    // With New(), each session has its own socket now.
    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 2)
    c.Assert(stats.SocketsInUse, Equals, 2)

    // Ensure query parameters were cloned.
    err = otherColl.Insert(M{"_id": 2})
    c.Assert(err, IsNil)

    mongogo.ResetStats()

    iter, err := otherColl.Find(M{}).Iter()
    c.Assert(err, IsNil)

    m := M{}
    err = iter.Next(m)
    c.Assert(err, IsNil)

    // If Batch(-1) is in effect, a single document must have been received.
    stats = mongogo.GetStats()
    c.Assert(stats.ReceivedDocs, Equals, 1)
}

func (s *S) TestCloneSession(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)
    defer session.Close()

    // Do a dummy operation to wait for connection.
    coll := session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"_id": 1})
    c.Assert(err, IsNil)

    // Tweak safety and query settings to ensure clone is copying those.
    session.Unsafe()
    session.Batch(-1)
    clone := session.Clone()
    defer clone.Close()
    session.Safe(0, 0, false)

    // Clone was copied while session was unsafe, so no errors.
    cloneColl := clone.DB("mydb").C("mycollection")
    err = cloneColl.Insert(M{"_id": 1})
    c.Assert(err, IsNil)

    // Original session was made safe again.
    err = coll.Insert(M{"_id": 1})
    c.Assert(err, NotNil)

    // With Clone(), same socket is shared between sessions now.
    stats := mongogo.GetStats()
    c.Assert(stats.SocketsInUse, Equals, 1)
    c.Assert(stats.SocketRefs, Equals, 2)

    // Restarting one of them should let the original socket go,
    // while preserving the safety settings.
    clone.Restart()
    err = cloneColl.Insert(M{"_id": 1})
    c.Assert(err, IsNil)

    // Must have used another connection now.
    stats = mongogo.GetStats()
    c.Assert(stats.SocketsInUse, Equals, 2)
    c.Assert(stats.SocketRefs, Equals, 2)

    // Ensure query parameters were cloned.
    err = cloneColl.Insert(M{"_id": 2})
    c.Assert(err, IsNil)

    mongogo.ResetStats()

    iter, err := cloneColl.Find(M{}).Iter()
    c.Assert(err, IsNil)

    m := M{}
    err = iter.Next(m)
    c.Assert(err, IsNil)

    // If Batch(-1) is in effect, a single document must have been received.
    stats = mongogo.GetStats()
    c.Assert(stats.ReceivedDocs, Equals, 1)
}

func (s *S) TestStrongSession(c *C) {
    session, err := mongogo.Mongo("localhost:40012")
    c.Assert(err, IsNil)
    defer session.Close()

    session.Monotonic()
    session.Strong()

    result := M{}
    cmd := session.DB("admin").C("$cmd")
    err = cmd.Find(M{"ismaster": 1}).One(&result)
    c.Assert(err, IsNil)
    c.Assert(result["ismaster"], Equals, true)

    coll := session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"a": 1})
    c.Assert(err, IsNil)

    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 2)
    c.Assert(stats.SocketsInUse, Equals, 1)
}

func (s *S) TestMonotonicSession(c *C) {
    // Must necessarily connect to a slave, otherwise the
    // master connection will be available first.
    session, err := mongogo.Mongo("localhost:40012")
    c.Assert(err, IsNil)
    defer session.Close()

    session.Monotonic()

    result := M{}
    cmd := session.DB("admin").C("$cmd")
    err = cmd.Find(M{"ismaster": 1}).One(&result)
    c.Assert(err, IsNil)
    c.Assert(result["ismaster"], Equals, false)

    coll := session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"a": 1})
    c.Assert(err, IsNil)

    result = M{}
    err = cmd.Find(M{"ismaster": 1}).One(&result)
    c.Assert(err, IsNil)
    c.Assert(result["ismaster"], Equals, true)

    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 2)
    c.Assert(stats.SocketsInUse, Equals, 1)
}

func (s *S) TestEventualSession(c *C) {
    // Must necessarily connect to a slave, otherwise the
    // master connection will be available first.
    session, err := mongogo.Mongo("localhost:40012")
    c.Assert(err, IsNil)
    defer session.Close()

    session.Eventual()

    result := M{}
    cmd := session.DB("admin").C("$cmd")
    err = cmd.Find(M{"ismaster": 1}).One(&result)
    c.Assert(err, IsNil)
    c.Assert(result["ismaster"], Equals, false)

    coll := session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"a": 1})
    c.Assert(err, IsNil)

    result = M{}
    err = cmd.Find(M{"ismaster": 1}).One(&result)
    c.Assert(err, IsNil)
    c.Assert(result["ismaster"], Equals, false)

    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 2)
    c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestPrimaryShutdownStrong(c *C) {
    if *fast {
        c.Skip("-fast")
    }

    session, err := mongogo.Mongo("localhost:40021")
    c.Assert(err, IsNil)
    defer session.Close()

    // With strong consistency, this will open a socket to the master.
    result := &struct{ Host string }{}
    err = session.Run("serverStatus", result)
    c.Assert(err, IsNil)

    // Kill the master.
    host := result.Host
    s.Stop(host)

    // This must fail, since the connection was broken.
    err = session.Run("serverStatus", result)
    c.Assert(err, Equals, os.EOF)

    // With strong consistency, it fails again until reset.
    err = session.Run("serverStatus", result)
    c.Assert(err, Equals, os.EOF)

    session.Restart()

    // Now we should be able to talk to the new master.
    err = session.Run("serverStatus", result)
    c.Assert(err, IsNil)
    c.Assert(result.Host, Not(Equals), host)
}

func (s *S) TestPrimaryShutdownMonotonic(c *C) {
    if *fast {
        c.Skip("-fast")
    }

    session, err := mongogo.Mongo("localhost:40021")
    c.Assert(err, IsNil)
    defer session.Close()

    session.Monotonic()

    // Insert something to force a switch to the master.
    coll := session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"a": 1})
    c.Assert(err, IsNil)

    result := &struct{ Host string }{}
    err = session.Run("serverStatus", result)
    c.Assert(err, IsNil)

    // Kill the master.
    host := result.Host
    s.Stop(host)

    // This must fail, since the connection was broken.
    err = session.Run("serverStatus", result)
    c.Assert(err, Equals, os.EOF)

    // With monotonic consistency, it fails again until reset.
    err = session.Run("serverStatus", result)
    c.Assert(err, Equals, os.EOF)

    session.Restart()

    // Now we should be able to talk to the new master.
    err = session.Run("serverStatus", result)
    c.Assert(err, IsNil)
    c.Assert(result.Host, Not(Equals), host)
}

func (s *S) TestPrimaryShutdownMonotonicWithSlave(c *C) {
    if *fast {
        c.Skip("-fast")
    }

    session, err := mongogo.Mongo("localhost:40021")
    c.Assert(err, IsNil)
    defer session.Close()

    ssresult := &struct{ Host string }{}
    imresult := &struct{ IsMaster bool }{}

    // Figure the master while still using the strong session.
    err = session.Run("serverStatus", ssresult)
    c.Assert(err, IsNil)
    err = session.Run("isMaster", imresult)
    c.Assert(err, IsNil)
    master := ssresult.Host
    c.Assert(imresult.IsMaster, Equals, true, Bug("%s is not the master", master))

    // Create new monotonic session with an explicit address to ensure
    // a slave is synchronized before the master, otherwise a connection
    // with the master may be used below for lack of other options.
    var addr string
    switch {
    case strings.HasSuffix(ssresult.Host, ":40021"):
        addr = "localhost:40022"
    case strings.HasSuffix(ssresult.Host, ":40022"):
        addr = "localhost:40021"
    case strings.HasSuffix(ssresult.Host, ":40023"):
        addr = "localhost:40021"
    default:
        c.Fatal("Unknown host: ", ssresult.Host)
    }

    session, err = mongogo.Mongo(addr)
    c.Assert(err, IsNil)
    defer session.Close()

    session.Monotonic()

    // Check the address of the socket associated with the monotonic session.
    c.Log("Running serverStatus and isMaster with monotonic session")
    err = session.Run("serverStatus", ssresult)
    c.Assert(err, IsNil)
    err = session.Run("isMaster", imresult)
    c.Assert(err, IsNil)
    slave := ssresult.Host
    c.Assert(imresult.IsMaster, Equals, false, Bug("%s is not a slave", slave))

    c.Assert(master, Not(Equals), slave)

    // Kill the master.
    s.Stop(master)

    // Session must still be good, since we were talking to a slave.
    err = session.Run("serverStatus", ssresult)
    c.Assert(err, IsNil)

    c.Assert(ssresult.Host, Equals, slave,
             Bug("Monotonic session moved from %s to %s", slave, ssresult.Host))

    // If we try to insert something, it'll have to hold until the new
    // master is available to move the connection, and work correctly.
    coll := session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"a": 1})
    c.Assert(err, IsNil)

    // Must now be talking to the new master.
    err = session.Run("serverStatus", ssresult)
    c.Assert(err, IsNil)
    err = session.Run("isMaster", imresult)
    c.Assert(err, IsNil)
    c.Assert(imresult.IsMaster, Equals, true, Bug("%s is not the master", master))

    // ... which is not the old one, since it's still dead.
    c.Assert(ssresult.Host, Not(Equals), master)
}

func (s *S) TestPrimaryShutdownEventual(c *C) {
    if *fast {
        c.Skip("-fast")
    }

    session, err := mongogo.Mongo("localhost:40021")
    c.Assert(err, IsNil)
    defer session.Close()

    result := &struct{ Host string }{}
    err = session.Run("serverStatus", result)
    c.Assert(err, IsNil)
    master := result.Host

    session.Eventual()

    // Should connect to the master when needed.
    coll := session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"a": 1})
    c.Assert(err, IsNil)

    // Kill the master.
    s.Stop(master)

    // Should still work, with the new master now.
    coll = session.DB("mydb").C("mycollection")
    err = coll.Insert(M{"a": 1})
    c.Assert(err, IsNil)

    err = session.Run("serverStatus", result)
    c.Assert(err, IsNil)
    c.Assert(result.Host, Not(Equals), master)
}

func (s *S) TestPreserveSocketCountOnSync(c *C) {
    if *fast {
        c.Skip("-fast")
    }

    session, err := mongogo.Mongo("localhost:40011")
    c.Assert(err, IsNil)
    defer session.Close()

    stats := mongogo.GetStats()
    for stats.MasterConns+stats.SlaveConns != 3 {
        stats = mongogo.GetStats()
        c.Log("Waiting for all connections to be established...")
        time.Sleep(5e8)
    }

    c.Assert(stats.SocketsAlive, Equals, 3)

    // Kill the master (with rs1, 'a' is always the master).
    s.Stop("localhost:40011")

    // Wait for the logic to run for a bit and bring it back.
    go func() {
        time.Sleep(5e9)
        s.StartAll()
    }()

    // Do an action to kick the resync logic in, and also to
    // wait until the cluster recognizes the server is back.
    result := struct{ Ok bool }{}
    err = session.Run("getLastError", &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, true)

    for i := 0; i != 20; i++ {
        stats = mongogo.GetStats()
        if stats.SocketsAlive == 3 {
            break
        }
        c.Logf("Waiting for 3 sockets alive, have %d", stats.SocketsAlive)
        time.Sleep(5e8)
    }

    // Ensure the number of sockets is preserved after syncing.
    stats = mongogo.GetStats()
    c.Assert(stats.SocketsAlive, Equals, 3)
    c.Assert(stats.SocketsInUse, Equals, 1)
    c.Assert(stats.SocketRefs, Equals, 1)
}

