// mgo - MongoDB driver for Go
// 
// Copyright (c) 2010-2011 - Gustavo Niemeyer <gustavo@niemeyer.net>
// 
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of the copyright holder nor the names of its
//       contributors may be used to endorse or promote products derived from
//       this software without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package mgo_test

import (
	"launchpad.net/gobson/bson"
	. "launchpad.net/gocheck"
	"launchpad.net/mgo"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Connect to the master of a deployment with a single server,
// run an insert, and then ensure the insert worked and that a
// single connection was established.
func (s *S) TestTopologySyncWithSingleMaster(c *C) {
	// Use hostname here rather than IP, to make things trickier.
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1, "b": 2})
	c.Assert(err, IsNil)

	// One connection used for discovery. Master socket recycled for
	// insert. Socket is reserved after insert.
	stats := mgo.GetStats()
	c.Assert(stats.MasterConns, Equals, 1)
	c.Assert(stats.SlaveConns, Equals, 0)
	c.Assert(stats.SocketsInUse, Equals, 1)

	// Refresh session and socket must be released.
	session.Refresh()
	stats = mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestTopologySyncWithSlaveSeed(c *C) {
	// That's supposed to be a slave. Must run discovery
	// and find out master to insert successfully.
	session, err := mgo.Mongo("localhost:40012")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"a": 1, "b": 2})

	result := struct{ Ok bool }{}
	err = session.Run("getLastError", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, true)

	// One connection to each during discovery. Master
	// socket recycled for insert. 
	stats := mgo.GetStats()
	c.Assert(stats.MasterConns, Equals, 1)
	c.Assert(stats.SlaveConns, Equals, 2)

	// Only one socket reference alive, in the master socket owned
	// by the above session.
	c.Assert(stats.SocketsInUse, Equals, 1)

	// Refresh it, and it must be gone.
	session.Refresh()
	stats = mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestRunString(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	result := struct{ Ok int }{}
	err = session.Run("ping", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestRunValue(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	result := struct{ Ok int }{}
	err = session.Run(M{"ping": 1}, &result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestPing(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	// Just ensure the nonce has been received.
	result := struct{}{}
	err = session.Run("ping", &result)

	mgo.ResetStats()

	err = session.Ping()
	c.Assert(err, IsNil)

	// Pretty boring.
	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 1)
	c.Assert(stats.ReceivedOps, Equals, 1)
}

func (s *S) TestURLSingle(c *C) {
	session, err := mgo.Mongo("mongodb://localhost:40001/")
	c.Assert(err, IsNil)
	defer session.Close()

	result := struct{ Ok int }{}
	err = session.Run("ping", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestURLMany(c *C) {
	session, err := mgo.Mongo("mongodb://localhost:40011,localhost:40012/")
	c.Assert(err, IsNil)
	defer session.Close()

	result := struct{ Ok int }{}
	err = session.Run("ping", &result)
	c.Assert(err, IsNil)
	c.Assert(result.Ok, Equals, 1)
}

func (s *S) TestInsertFindOne(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"a": 1, "b": 2})

	result := struct{ A, B int }{}

	err = coll.Find(M{"a": 1}).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.A, Equals, 1)
	c.Assert(result.B, Equals, 2)
}

func (s *S) TestInsertFindOneMap(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"a": 1, "b": 2})
	result := make(M)
	err = coll.Find(M{"a": 1}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["a"], Equals, 1)
	c.Assert(result["b"], Equals, 2)
}

func (s *S) TestSelect(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"a": 1, "b": 2})

	result := struct{ A, B int }{}

	err = coll.Find(M{"a": 1}).Select(M{"b": 1}).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.A, Equals, 0)
	c.Assert(result.B, Equals, 2)
}

func (s *S) TestUpdate(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"k": n, "n": n})
		c.Assert(err, IsNil)
	}

	err = coll.Update(M{"k": 42}, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	result := make(M)
	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 43)

	err = coll.Update(M{"k": 47}, M{"k": 47, "n": 47})
	c.Assert(err, Equals, nil)

	err = coll.Find(M{"k": 47}).One(result)
	c.Assert(err, Equals, mgo.NotFound)
}

func (s *S) TestUpsert(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"k": n, "n": n})
		c.Assert(err, IsNil)
	}

	err = coll.Upsert(M{"k": 42}, M{"k": 42, "n": 24})
	c.Assert(err, IsNil)

	result := make(M)
	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 24)

	err = coll.Upsert(M{"k": 47}, M{"k": 47, "n": 47})
	c.Assert(err, IsNil)

	err = coll.Find(M{"k": 47}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 47)
}

func (s *S) TestUpdateAll(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"k": n, "n": n})
		c.Assert(err, IsNil)
	}

	err = coll.UpdateAll(M{"k": M{"$gt": 42}}, M{"$inc": M{"n": 1}})
	c.Assert(err, IsNil)

	result := make(M)
	err = coll.Find(M{"k": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 42)

	err = coll.Find(M{"k": 43}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 44)

	err = coll.Find(M{"k": 44}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result["n"], Equals, 45)
}

func (s *S) TestRemove(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	err = coll.Remove(M{"n": M{"$gt": 42}})
	c.Assert(err, IsNil)

	result := &struct{ N int }{}
	err = coll.Find(M{"n": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 42)

	err = coll.Find(M{"n": 43}).One(result)
	c.Assert(err, Equals, mgo.NotFound)

	err = coll.Find(M{"n": 44}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 44)
}

func (s *S) TestRemoveAll(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	err = coll.RemoveAll(M{"n": M{"$gt": 42}})
	c.Assert(err, IsNil)

	result := &struct{ N int }{}
	err = coll.Find(M{"n": 42}).One(result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 42)

	err = coll.Find(M{"n": 43}).One(result)
	c.Assert(err, Equals, mgo.NotFound)

	err = coll.Find(M{"n": 44}).One(result)
	c.Assert(err, Equals, mgo.NotFound)
}

func (s *S) TestCountCollection(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	n, err := coll.Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 3)
}

func (s *S) TestCountQuery(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	n, err := coll.Find(M{"n": M{"$gt": 40}}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)
}

func (s *S) TestCountQuerySorted(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42}
	for _, n := range ns {
		err := coll.Insert(M{"n": n})
		c.Assert(err, IsNil)
	}

	n, err := coll.Find(M{"n": M{"$gt": 40}}).Sort(M{"n": 1}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 2)
}

func (s *S) TestFindOneNotFound(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	result := struct{ A, B int }{}
	err = coll.Find(M{"a": 1}).One(&result)
	c.Assert(err, Equals, mgo.NotFound)
	c.Assert(err, Matches, "Document not found")
	c.Assert(err == mgo.NotFound, Equals, true)
}

func (s *S) TestFindNil(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"n": 1})

	result := struct{ N int }{}

	err = coll.Find(nil).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 1)
}

func (s *S) TestFindIterAll(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort(M{"$natural": 1}).Prefetch(0).Batch(2)
	iter, err := query.Iter()
	c.Assert(err, IsNil)

	result := struct{ N int }{}
	for i := 2; i < 7; i++ {
		err = iter.Next(&result)
		c.Assert(err, IsNil)
		c.Assert(result.N, Equals, ns[i])
		if i == 1 {
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	err = iter.Next(&result)
	c.Assert(err == mgo.NotFound, Equals, true)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 2*GET_MORE_OP
	c.Assert(stats.ReceivedOps, Equals, 3) // and their REPLY_OPs.
	c.Assert(stats.ReceivedDocs, Equals, 5)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestFindIterTwiceWithSameQuery(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

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

func (s *S) TestFindIterWithoutResults(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	coll.Insert(M{"n": 42})

	iter, err := coll.Find(M{"n": 0}).Iter()
	c.Assert(err, IsNil)

	result := struct{ N int }{}
	err = iter.Next(&result)
	c.Assert(result.N, Equals, 0)
	c.Assert(err == mgo.NotFound, Equals, true)
}

func (s *S) TestFindIterLimit(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort(M{"$natural": 1}).Limit(3)
	iter, err := query.Iter()
	c.Assert(err, IsNil)

	result := struct{ N int }{}
	for i := 2; i < 5; i++ {
		err = iter.Next(&result)
		c.Assert(err, IsNil)
		c.Assert(result.N, Equals, ns[i])
	}

	err = iter.Next(&result)
	c.Assert(err == mgo.NotFound, Equals, true)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 1)     // 1*QUERY_OP
	c.Assert(stats.ReceivedOps, Equals, 1) // and its REPLY_OP
	c.Assert(stats.ReceivedDocs, Equals, 3)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestFindIterLimitWithBatch(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort(M{"$natural": 1}).Limit(3).Batch(2)
	iter, err := query.Iter()
	c.Assert(err, IsNil)

	result := struct{ N int }{}
	for i := 2; i < 5; i++ {
		err = iter.Next(&result)
		c.Assert(err, IsNil)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 {
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	err = iter.Next(&result)
	c.Assert(err == mgo.NotFound, Equals, true)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 2)     // 1*QUERY_OP + 1*GET_MORE_OP
	c.Assert(stats.ReceivedOps, Equals, 2) // and its REPLY_OPs
	c.Assert(stats.ReceivedDocs, Equals, 3)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

// Test tailable cursors in a situation where Next has to sleep to
// respect the timeout requested on Tail.
func (s *S) TestFindTailTimeoutWithSleep(c *C) {
	if *fast {
		c.Skip("-fast")
	}

	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	cresult := struct{ ErrMsg string }{}

	db := session.DB("mydb")
	err = db.Run(bson.D{{"create", "mycoll"}, {"capped", true}, {"size", 1024}}, &cresult)
	c.Assert(err, IsNil)
	c.Assert(cresult.ErrMsg, Equals, "")
	coll := db.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	const timeout = 3

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort(M{"$natural": 1}).Prefetch(0).Batch(2)
	iter, err := query.Tail(timeout)
	c.Assert(err, IsNil)

	n := len(ns)
	result := struct{ N int }{}
	for i := 2; i != n; i++ {
		err = iter.Next(&result)
		c.Assert(err, IsNil)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 { // The batch boundary.
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	mgo.ResetStats()

	// The following call to Next will block.
	go func() {
		// The internal AwaitData timing of MongoDB is around 2 seconds,
		// so this should force mgo to sleep at least once by itself to
		// respect the requested timeout.
		time.Sleep(timeout*1e9 + 5e8)
		session := session.New()
		defer session.Close()
		coll := session.DB("mydb").C("mycoll")
		coll.Insert(M{"n": 47})
	}()

	c.Log("Will wait for Next with N=47...")
	err = iter.Next(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 47)
	c.Log("Got Next with N=47!")

	// The following may break because it depends a bit on the internal
	// timing used by MongoDB's AwaitData logic.  If it does, the problem
	// will be observed as more GET_MORE_OPs than predicted:
	// 1*QUERY for nonce + 1*GET_MORE_OP on Next + 1*GET_MORE_OP on Next after sleep +
	// 1*INSERT_OP + 1*QUERY_OP for getLastError on insert of 47
	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 5)
	c.Assert(stats.ReceivedOps, Equals, 4)  // REPLY_OPs for 1*QUERY_OP for nonce + 2*GET_MORE_OPs + 1*QUERY_OP
	c.Assert(stats.ReceivedDocs, Equals, 3) // nonce + N=47 result + getLastError response

	c.Log("Will wait for a result which will never come...")

	started := time.Nanoseconds()
	err = iter.Next(&result)
	c.Assert(time.Nanoseconds()-started > timeout*1e9, Equals, true)
	c.Assert(err == mgo.TailTimeout, Equals, true)
}

// Test tailable cursors in a situation where Next never gets to sleep once
// to respect the timeout requested on Tail.
func (s *S) TestFindTailTimeoutNoSleep(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	cresult := struct{ ErrMsg string }{}

	db := session.DB("mydb")
	err = db.Run(bson.D{{"create", "mycoll"}, {"capped", true}, {"size", 1024}}, &cresult)
	c.Assert(err, IsNil)
	c.Assert(cresult.ErrMsg, Equals, "")
	coll := db.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	const timeout = 1

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort(M{"$natural": 1}).Prefetch(0).Batch(2)
	iter, err := query.Tail(timeout)
	c.Assert(err, IsNil)

	n := len(ns)
	result := struct{ N int }{}
	for i := 2; i != n; i++ {
		err = iter.Next(&result)
		c.Assert(err, IsNil)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 { // The batch boundary.
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	mgo.ResetStats()

	// The following call to Next will block.
	go func() {
		// The internal AwaitData timing of MongoDB is around 2 seconds,
		// so this item should arrive within the AwaitData threshold.
		time.Sleep(5e8)
		session := session.New()
		defer session.Close()
		coll := session.DB("mydb").C("mycoll")
		coll.Insert(M{"n": 47})
	}()

	c.Log("Will wait for Next with N=47...")
	err = iter.Next(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 47)
	c.Log("Got Next with N=47!")

	// The following may break because it depends a bit on the internal
	// timing used by MongoDB's AwaitData logic.  If it does, the problem
	// will be observed as more GET_MORE_OPs than predicted:
	// 1*QUERY_OP for nonce + 1*GET_MORE_OP on Next +
	// 1*INSERT_OP + 1*QUERY_OP for getLastError on insert of 47
	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 4)
	c.Assert(stats.ReceivedOps, Equals, 3)  // REPLY_OPs for 1*QUERY_OP for nonce + 1*GET_MORE_OPs and 1*QUERY_OP
	c.Assert(stats.ReceivedDocs, Equals, 3) // nonce + N=47 result + getLastError response

	c.Log("Will wait for a result which will never come...")

	started := time.Nanoseconds()
	err = iter.Next(&result)
	c.Assert(time.Nanoseconds()-started > timeout*1e9, Equals, true)
	c.Assert(err == mgo.TailTimeout, Equals, true)
}

// Test tailable cursors in a situation where Next never gets to sleep once
// to respect the timeout requested on Tail.
func (s *S) TestFindTailNoTimeout(c *C) {
	if *fast {
		c.Skip("-fast")
	}

	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	cresult := struct{ ErrMsg string }{}

	db := session.DB("mydb")
	err = db.Run(bson.D{{"create", "mycoll"}, {"capped", true}, {"size", 1024}}, &cresult)
	c.Assert(err, IsNil)
	c.Assert(cresult.ErrMsg, Equals, "")
	coll := db.C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort(M{"$natural": 1}).Prefetch(0).Batch(2)
	iter, err := query.Tail(-1)
	c.Assert(err, IsNil)

	n := len(ns)
	result := struct{ N int }{}
	for i := 2; i != n; i++ {
		err = iter.Next(&result)
		c.Assert(err, IsNil)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 { // The batch boundary.
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
	}

	mgo.ResetStats()

	// The following call to Next will block.
	go func() {
		time.Sleep(5e8)
		session := session.New()
		defer session.Close()
		coll := session.DB("mydb").C("mycoll")
		coll.Insert(M{"n": 47})
	}()

	c.Log("Will wait for Next with N=47...")
	err = iter.Next(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 47)
	c.Log("Got Next with N=47!")

	// The following may break because it depends a bit on the internal
	// timing used by MongoDB's AwaitData logic.  If it does, the problem
	// will be observed as more GET_MORE_OPs than predicted:
	// 1*QUERY_OP for nonce + 1*GET_MORE_OP on Next +
	// 1*INSERT_OP + 1*QUERY_OP for getLastError on insert of 47
	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 4)
	c.Assert(stats.ReceivedOps, Equals, 3)  // REPLY_OPs for 1*QUERY_OP for nonce + 1*GET_MORE_OPs and 1*QUERY_OP
	c.Assert(stats.ReceivedDocs, Equals, 3) // nonce + N=47 result + getLastError response

	c.Log("Will wait for a result which will never come...")

	gotNext := make(chan os.Error)
	go func() {
		err := iter.Next(&result)
		gotNext <- err
	}()

	select {
	case err := <-gotNext:
		c.Fatal("Next returned: " + err.String())
	case <-time.After(3e9):
		// Good. Should still be sleeping at that point.
	}

	// Closing the session should cause Next to return.
	session.Close()

	select {
	case err := <-gotNext:
		c.Assert(err, Matches, "Closed explicitly")
	case <-time.After(1e9):
		c.Fatal("Closing the session did not unblock Next")
	}
}

func (s *S) TestFindForOnIter(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort(M{"$natural": 1}).Prefetch(0).Batch(2)
	iter, err := query.Iter()
	c.Assert(err, IsNil)

	i := 2
	var result *struct{ N int }
	err = iter.For(&result, func() os.Error {
		c.Assert(i < 7, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 1 {
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
		i++
		return nil
	})
	c.Assert(err == mgo.NotFound, Equals, true)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 2*GET_MORE_OP
	c.Assert(stats.ReceivedOps, Equals, 3) // and their REPLY_OPs.
	c.Assert(stats.ReceivedDocs, Equals, 5)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestFindFor(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	session.Refresh() // Release socket.

	mgo.ResetStats()

	query := coll.Find(M{"n": M{"$gte": 42}}).Sort(M{"$natural": 1}).Prefetch(0).Batch(2)

	i := 2
	var result *struct{ N int }
	err = query.For(&result, func() os.Error {
		c.Assert(i < 7, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 1 {
			stats := mgo.GetStats()
			c.Assert(stats.ReceivedDocs, Equals, 2)
		}
		i++
		return nil
	})
	c.Assert(err == mgo.NotFound, Equals, true)

	session.Refresh() // Release socket.

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 3)     // 1*QUERY_OP + 2*GET_MORE_OP
	c.Assert(stats.ReceivedOps, Equals, 3) // and their REPLY_OPs.
	c.Assert(stats.ReceivedDocs, Equals, 5)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestFindForStopOnError(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{40, 41, 42, 43, 44, 45, 46}
	for _, n := range ns {
		coll.Insert(M{"n": n})
	}

	query := coll.Find(M{"n": M{"$gte": 42}})
	i := 2
	var result *struct{ N int }
	err = query.For(&result, func() os.Error {
		c.Assert(i < 4, Equals, true)
		c.Assert(result.N, Equals, ns[i])
		if i == 3 {
			return os.ErrorString("stop!")
		}
		i++
		return nil
	})
	c.Assert(err, Matches, "stop!")
}

func (s *S) TestFindForResetsResult(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	ns := []int{1, 2, 3}
	for _, n := range ns {
		coll.Insert(M{"n" + strconv.Itoa(n): n})
	}

	query := coll.Find(nil).Sort(M{"$natural": 1})

	i := 0
	var sresult *struct{ N1, N2, N3 int }
	err = query.For(&sresult, func() os.Error {
		switch i {
		case 0:
			c.Assert(sresult.N1, Equals, 1)
			c.Assert(sresult.N2+sresult.N3, Equals, 0)
		case 1:
			c.Assert(sresult.N2, Equals, 2)
			c.Assert(sresult.N1+sresult.N3, Equals, 0)
		case 2:
			c.Assert(sresult.N3, Equals, 3)
			c.Assert(sresult.N1+sresult.N2, Equals, 0)
		}
		i++
		return nil
	})
	c.Assert(err == mgo.NotFound, Equals, true)

	i = 0
	var mresult M
	err = query.For(&mresult, func() os.Error {
		mresult["_id"] = nil, false
		switch i {
		case 0:
			c.Assert(mresult, Equals, M{"n1": 1})
		case 1:
			c.Assert(mresult, Equals, M{"n2": 2})
		case 2:
			c.Assert(mresult, Equals, M{"n3": 3})
		}
		i++
		return nil
	})
	c.Assert(err == mgo.NotFound, Equals, true)

	i = 0
	var iresult interface{}
	err = query.For(&iresult, func() os.Error {
		mresult, ok := iresult.(bson.M)
		c.Assert(ok, Equals, true, Bug("%#v", iresult))
		mresult["_id"] = nil, false
		switch i {
		case 0:
			c.Assert(mresult, Equals, bson.M{"n1": 1})
		case 1:
			c.Assert(mresult, Equals, bson.M{"n2": 2})
		case 2:
			c.Assert(mresult, Equals, bson.M{"n3": 3})
		}
		i++
		return nil
	})
	c.Assert(err == mgo.NotFound, Equals, true)
}

func (s *S) TestSort(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

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
	query.Sort(bson.D{{"a", -1}}) // Should be ignored.
	iter, err := query.Sort(bson.D{{"b", -1}, {"a", 1}}).Iter()
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

func (s *S) TestPrefetching(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	docs := make([]interface{}, 200)
	for i := 0; i != 200; i++ {
		docs[i] = M{"n": i}
	}
	coll.Insert(docs...)

	// Same test three times.  Once with prefetching via query, then with the
	// default prefetching, and a third time tweaking the default settings in
	// the session.
	for testi := 0; testi != 3; testi++ {
		mgo.ResetStats()

		var iter *mgo.Iter
		var nextn int

		switch testi {
		case 0: // First, using query methods.
			iter, err = coll.Find(M{}).Prefetch(0.27).Batch(100).Iter()
			c.Assert(err, IsNil)
			nextn = 73

		case 1: // Then, the default session value.
			session.SetBatch(100)
			iter, err = coll.Find(M{}).Iter()
			c.Assert(err, IsNil)
			nextn = 75

		case 2: // Then, tweaking the session value.
			session.SetBatch(100)
			session.SetPrefetch(0.27)
			iter, err = coll.Find(M{}).Iter()
			c.Assert(err, IsNil)
			nextn = 73
		}

		result := struct{ N int }{}
		for i := 0; i != nextn; i++ {
			iter.Next(&result)
		}

		stats := mgo.GetStats()
		c.Assert(stats.ReceivedDocs, Equals, 100)

		iter.Next(&result)

		// Ping the database just to wait for the fetch above
		// to get delivered.
		session.Run("ping", M{}) // XXX Should support nil here.

		stats = mgo.GetStats()
		c.Assert(stats.ReceivedDocs, Equals, 201) // 200 + the ping result
	}
}

func (s *S) TestSafeSetting(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	// Check the default
	safe := session.Safe()
	c.Assert(safe.W, Equals, 0)
	c.Assert(safe.WTimeout, Equals, 0)
	c.Assert(safe.FSync, Equals, false)

	// Tweak it
	session.SetSafe(&mgo.Safe{W: 1, WTimeout: 2, FSync: true})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 1)
	c.Assert(safe.WTimeout, Equals, 2)
	c.Assert(safe.FSync, Equals, true)

	// Reset it again.
	session.SetSafe(&mgo.Safe{})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 0)
	c.Assert(safe.WTimeout, Equals, 0)
	c.Assert(safe.FSync, Equals, false)

	// Ensure safety to something higher.
	session.SetSafe(&mgo.Safe{W: 5, WTimeout: 6, FSync: true})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 5)
	c.Assert(safe.WTimeout, Equals, 6)
	c.Assert(safe.FSync, Equals, true)

	// Ensure safety to something less conservative won't change it.
	session.EnsureSafe(&mgo.Safe{W: 4, WTimeout: 7, FSync: false})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 5)
	c.Assert(safe.WTimeout, Equals, 6)
	c.Assert(safe.FSync, Equals, true)

	// But to something more conservative will.
	session.EnsureSafe(&mgo.Safe{W: 6, WTimeout: 4})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 6)
	c.Assert(safe.WTimeout, Equals, 4)
	c.Assert(safe.FSync, Equals, true)

	// EnsureSafe with nil does nothing.
	session.EnsureSafe(nil)
	safe = session.Safe()
	c.Assert(safe.W, Equals, 6)
	c.Assert(safe.WTimeout, Equals, 4)
	c.Assert(safe.FSync, Equals, true)

	// Changing the safety of a cloned session doesn't touch the original.
	clone := session.Clone()
	defer clone.Close()
	clone.EnsureSafe(&mgo.Safe{W: 100})
	safe = session.Safe()
	c.Assert(safe.W, Equals, 6)
}

func (s *S) TestSafeInsert(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	// Insert an element with a predefined key.
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	mgo.ResetStats()

	// Session should be safe by default, so inserting it again must fail.
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, Matches, "E11000 duplicate.*")
	c.Assert(err.(*mgo.LastError).Code, Equals, 11000)

	// It must have sent two operations (INSERT_OP + getLastError QUERY_OP)
	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 2)

	mgo.ResetStats()

	// If we disable safety, though, it won't complain.
	session.SetSafe(nil)
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	// Must have sent a single operation this time (just the INSERT_OP)
	stats = mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 1)
}

func (s *S) TestSafeParameters(c *C) {
	session, err := mgo.Mongo("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	// Tweak the safety parameters to something unachievable.
	session.SetSafe(&mgo.Safe{4, 100, false})
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, Matches, "timeout")
	c.Assert(err.(*mgo.LastError).WTimeout, Equals, true)
}

func (s *S) TestQueryErrorOne(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	result := struct {
		Err string "$err"
	}{}

	err = coll.Find(M{"a": 1}).Select(M{"a": M{"b": 1}}).One(&result)
	c.Assert(err, Matches, "Unsupported projection option: b")
	c.Assert(err.(*mgo.QueryError).Err, Matches, "Unsupported projection option: b")
	c.Assert(err.(*mgo.QueryError).Code, Equals, 13097)

	// The result should be properly unmarshalled with QueryError
	c.Assert(result.Err, Matches, "Unsupported projection option: b")
}

func (s *S) TestQueryErrorNext(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	result := struct {
		Err string "$err"
	}{}

	iter, err := coll.Find(M{"a": 1}).Select(M{"a": M{"b": 1}}).Iter()
	c.Assert(err, IsNil)

	err = iter.Next(&result)
	c.Assert(err, Matches, "Unsupported projection option: b")
	c.Assert(err.(*mgo.QueryError).Err, Matches, "Unsupported projection option: b")
	c.Assert(err.(*mgo.QueryError).Code, Equals, 13097)

	// The result should be properly unmarshalled with QueryError
	c.Assert(result.Err, Matches, "Unsupported projection option: b")
}

func (s *S) TestEnsureIndex(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	index1 := mgo.Index{
		Key:        []string{"a"},
		Background: true,
	}

	index2 := mgo.Index{
		Key:      []string{"a", "-b"},
		Unique:   true,
		DropDups: true,
	}

	index3 := mgo.Index{
		Key:  []string{"@loc"},
		Min:  -500,
		Max:  500,
		Bits: 32,
	}

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndex(index1)
	c.Assert(err, IsNil)

	err = coll.EnsureIndex(index2)
	c.Assert(err, IsNil)

	err = coll.EnsureIndex(index3)
	c.Assert(err, IsNil)

	sysidx := session.DB("mydb").C("system.indexes")

	result1 := M{}
	err = sysidx.Find(M{"name": "a_1"}).One(result1)
	c.Assert(err, IsNil)

	result2 := M{}
	err = sysidx.Find(M{"name": "a_1_b_-1"}).One(result2)
	c.Assert(err, IsNil)

	result3 := M{}
	err = sysidx.Find(M{"name": "loc_"}).One(result3)
	c.Assert(err, IsNil)

	expected1 := M{
		"name":       "a_1",
		"key":        bson.M{"a": 1},
		"ns":         "mydb.mycoll",
		"v":          0,
		"background": true,
	}
	c.Assert(result1, Equals, expected1)

	expected2 := M{
		"name":     "a_1_b_-1",
		"key":      bson.M{"a": 1, "b": -1},
		"ns":       "mydb.mycoll",
		"unique":   true,
		"dropDups": true,
		"v":        0,
	}
	c.Assert(result2, Equals, expected2)

	expected3 := M{
		"name": "loc_",
		"key":  bson.M{"loc": "2d"},
		"ns":   "mydb.mycoll",
		"min":  -500,
		"max":  500,
		"bits": 32,
	}
	c.Assert(result3, Equals, expected3)

	// Ensure the index actually works for real.
	err = coll.Insert(M{"a": 1, "b": 1})
	c.Assert(err, IsNil)
	err = coll.Insert(M{"a": 1, "b": 1})
	c.Assert(err, Matches, ".*duplicate key error.*")
}

func (s *S) TestEnsureIndexWithBadInfo(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndex(mgo.Index{})
	c.Assert(err, Matches, "Invalid index key:.*")

	err = coll.EnsureIndex(mgo.Index{Key: []string{""}})
	c.Assert(err, Matches, "Invalid index key:.*")
}

func (s *S) TestEnsureIndexWithUnsafeSession(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetSafe(nil)

	coll := session.DB("mydb").C("mycoll")

	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	// Should fail since there are duplicated entries.
	index := mgo.Index{
		Key:    []string{"a"},
		Unique: true,
	}

	err = coll.EnsureIndex(index)
	c.Assert(err, Matches, ".*duplicate key error.*")
}

func (s *S) TestEnsureIndexKey(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndexKey([]string{"a"})
	c.Assert(err, IsNil)

	err = coll.EnsureIndexKey([]string{"a", "-b"})
	c.Assert(err, IsNil)

	sysidx := session.DB("mydb").C("system.indexes")

	result1 := M{}
	err = sysidx.Find(M{"name": "a_1"}).One(result1)
	c.Assert(err, IsNil)

	result2 := M{}
	err = sysidx.Find(M{"name": "a_1_b_-1"}).One(result2)
	c.Assert(err, IsNil)

	expected1 := M{
		"name": "a_1",
		"key":  bson.M{"a": 1},
		"ns":   "mydb.mycoll",
		"v":    0,
	}
	c.Assert(result1, Equals, expected1)

	expected2 := M{
		"name": "a_1_b_-1",
		"key":  bson.M{"a": 1, "b": -1},
		"ns":   "mydb.mycoll",
		"v":    0,
	}
	c.Assert(result2, Equals, expected2)
}

func (s *S) TestEnsureIndexDropIndex(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndexKey([]string{"a"})
	c.Assert(err, IsNil)

	err = coll.EnsureIndexKey([]string{"-b"})
	c.Assert(err, IsNil)

	err = coll.DropIndex([]string{"-b"})
	c.Assert(err, IsNil)

	sysidx := session.DB("mydb").C("system.indexes")
	dummy := &struct{}{}

	err = sysidx.Find(M{"name": "a_1"}).One(dummy)
	c.Assert(err, IsNil)

	err = sysidx.Find(M{"name": "b_1"}).One(dummy)
	c.Assert(err, Equals, mgo.NotFound)

	err = coll.DropIndex([]string{"a"})
	c.Assert(err, IsNil)

	err = sysidx.Find(M{"name": "a_1"}).One(dummy)
	c.Assert(err, Equals, mgo.NotFound)

	err = coll.DropIndex([]string{"a"})
	c.Assert(err, Matches, "index not found")
}

func (s *S) TestEnsureIndexCaching(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndexKey([]string{"a"})
	c.Assert(err, IsNil)

	mgo.ResetStats()

	// Second EnsureIndex should be cached and do nothing.
	err = coll.EnsureIndexKey([]string{"a"})
	c.Assert(err, IsNil)

	stats := mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 0)

	// Resetting the cache should make it contact the server again.
	session.ResetIndexCache()

	err = coll.EnsureIndexKey([]string{"a"})
	c.Assert(err, IsNil)

	stats = mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 2)

	// Dropping the index should also drop the cached index key.
	err = coll.DropIndex([]string{"a"})
	c.Assert(err, IsNil)

	mgo.ResetStats()

	err = coll.EnsureIndexKey([]string{"a"})
	c.Assert(err, IsNil)

	stats = mgo.GetStats()
	c.Assert(stats.SentOps, Equals, 2)
}

func (s *S) TestEnsureIndexGetIndexes(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")

	err = coll.EnsureIndexKey([]string{"-b"})
	c.Assert(err, IsNil)

	err = coll.EnsureIndexKey([]string{"a"})
	c.Assert(err, IsNil)

	err = coll.EnsureIndexKey([]string{"@c"})
	c.Assert(err, IsNil)

	indexes, err := coll.Indexes()

	c.Assert(indexes[0].Name, Equals, "_id_")
	c.Assert(indexes[1].Name, Equals, "a_1")
	c.Assert(indexes[1].Key, Equals, []string{"a"})
	c.Assert(indexes[2].Name, Equals, "b_-1")
	c.Assert(indexes[2].Key, Equals, []string{"-b"})
	c.Assert(indexes[3].Name, Equals, "c_")
	c.Assert(indexes[3].Key, Equals, []string{"@c"})
}

func (s *S) TestNewSession(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	// Do a dummy operation to wait for connection.
	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	// Tweak safety and query settings to ensure other has copied those.
	session.SetSafe(nil)
	session.SetBatch(-1)
	other := session.New()
	defer other.Close()
	session.SetSafe(&mgo.Safe{0, 0, false})

	// Clone was copied while session was unsafe, so no errors.
	otherColl := other.DB("mydb").C("mycoll")
	err = otherColl.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	// Original session was made safe again.
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, NotNil)

	// With New(), each session has its own socket now.
	stats := mgo.GetStats()
	c.Assert(stats.MasterConns, Equals, 2)
	c.Assert(stats.SocketsInUse, Equals, 2)

	// Ensure query parameters were cloned.
	err = otherColl.Insert(M{"_id": 2})
	c.Assert(err, IsNil)

	// Ping the database to ensure the nonce has been received already.
	c.Assert(other.Ping(), IsNil)

	mgo.ResetStats()

	iter, err := otherColl.Find(M{}).Iter()
	c.Assert(err, IsNil)

	m := M{}
	err = iter.Next(m)
	c.Assert(err, IsNil)

	// If Batch(-1) is in effect, a single document must have been received.
	stats = mgo.GetStats()
	c.Assert(stats.ReceivedDocs, Equals, 1)
}

func (s *S) TestCloneSession(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	// Do a dummy operation to wait for connection.
	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	// Tweak safety and query settings to ensure clone is copying those.
	session.SetSafe(nil)
	session.SetBatch(-1)
	clone := session.Clone()
	defer clone.Close()
	session.SetSafe(&mgo.Safe{0, 0, false})

	// Clone was copied while session was unsafe, so no errors.
	cloneColl := clone.DB("mydb").C("mycoll")
	err = cloneColl.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	// Original session was made safe again.
	err = coll.Insert(M{"_id": 1})
	c.Assert(err, NotNil)

	// With Clone(), same socket is shared between sessions now.
	stats := mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 1)
	c.Assert(stats.SocketRefs, Equals, 2)

	// Refreshing one of them should let the original socket go,
	// while preserving the safety settings.
	clone.Refresh()
	err = cloneColl.Insert(M{"_id": 1})
	c.Assert(err, IsNil)

	// Must have used another connection now.
	stats = mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 2)
	c.Assert(stats.SocketRefs, Equals, 2)

	// Ensure query parameters were cloned.
	err = cloneColl.Insert(M{"_id": 2})
	c.Assert(err, IsNil)

	// Ping the database to ensure the nonce has been received already.
	c.Assert(clone.Ping(), IsNil)

	mgo.ResetStats()

	iter, err := cloneColl.Find(M{}).Iter()
	c.Assert(err, IsNil)

	m := M{}
	err = iter.Next(m)
	c.Assert(err, IsNil)

	// If Batch(-1) is in effect, a single document must have been received.
	stats = mgo.GetStats()
	c.Assert(stats.ReceivedDocs, Equals, 1)
}

func (s *S) TestSetModeStrong(c *C) {
	session, err := mgo.Mongo("localhost:40012")
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetMode(mgo.Monotonic, false)
	session.SetMode(mgo.Strong, false)

	c.Assert(session.Mode(), Equals, mgo.Strong)

	result := M{}
	cmd := session.DB("admin").C("$cmd")
	err = cmd.Find(M{"ismaster": 1}).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result["ismaster"], Equals, true)

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	// Wait since the sync also uses sockets.
	for len(session.GetLiveServers()) != 3 {
		c.Log("Waiting for cluster sync to finish...")
		time.Sleep(5e8)
	}

	stats := mgo.GetStats()
	c.Assert(stats.MasterConns, Equals, 1)
	c.Assert(stats.SlaveConns, Equals, 2)
	c.Assert(stats.SocketsInUse, Equals, 1)

	session.SetMode(mgo.Strong, true)

	stats = mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestSetModeMonotonic(c *C) {
	// Must necessarily connect to a slave, otherwise the
	// master connection will be available first.
	session, err := mgo.Mongo("localhost:40012")
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetMode(mgo.Monotonic, false)

	c.Assert(session.Mode(), Equals, mgo.Monotonic)

	result := M{}
	cmd := session.DB("admin").C("$cmd")
	err = cmd.Find(M{"ismaster": 1}).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result["ismaster"], Equals, false)

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	result = M{}
	err = cmd.Find(M{"ismaster": 1}).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result["ismaster"], Equals, true)

	// Wait since the sync also uses sockets.
	for len(session.GetLiveServers()) != 3 {
		c.Log("Waiting for cluster sync to finish...")
		time.Sleep(5e8)
	}

	stats := mgo.GetStats()
	c.Assert(stats.MasterConns, Equals, 1)
	c.Assert(stats.SlaveConns, Equals, 2)
	c.Assert(stats.SocketsInUse, Equals, 1)

	session.SetMode(mgo.Monotonic, true)

	stats = mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestSetModeMonotonicAfterStrong(c *C) {
	// Test that a strong session shifting to a monotonic
	// one preserves the socket untouched.

	session, err := mgo.Mongo("localhost:40012")
	c.Assert(err, IsNil)
	defer session.Close()

	// Insert something to force a connection to the master.
	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	session.SetMode(mgo.Monotonic, false)

	// Wait since the sync also uses sockets.
	for len(session.GetLiveServers()) != 3 {
		c.Log("Waiting for cluster sync to finish...")
		time.Sleep(5e8)
	}

	// Master socket should still be reserved.
	stats := mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 1)

	// Confirm it's the master even though it's Monotonic by now.
	result := M{}
	cmd := session.DB("admin").C("$cmd")
	err = cmd.Find(M{"ismaster": 1}).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result["ismaster"], Equals, true)
}

func (s *S) TestSetModeStrongAfterMonotonic(c *C) {
	// Test that shifting from Monotonic to Strong while
	// using a slave socket will keep the socket reserved
	// until the master socket is necessary, so that no
	// switch over occurs unless it's actually necessary.

	// Must necessarily connect to a slave, otherwise the
	// master connection will be available first.
	session, err := mgo.Mongo("localhost:40012")
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetMode(mgo.Monotonic, false)

	// Ensure we're talking to a slave, and reserve the socket.
	result := M{}
	err = session.Run("ismaster", &result)
	c.Assert(err, IsNil)
	c.Assert(result["ismaster"], Equals, false)

	// Switch to a Strong session.
	session.SetMode(mgo.Strong, false)

	// Wait since the sync also uses sockets.
	for len(session.GetLiveServers()) != 3 {
		c.Log("Waiting for cluster sync to finish...")
		time.Sleep(5e8)
	}

	// Slave socket should still be reserved.
	stats := mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 1)

	// But any operation will switch it to the master.
	result = M{}
	err = session.Run("ismaster", &result)
	c.Assert(err, IsNil)
	c.Assert(result["ismaster"], Equals, true)
}

func (s *S) TestSetModeEventual(c *C) {
	// Must necessarily connect to a slave, otherwise the
	// master connection will be available first.
	session, err := mgo.Mongo("localhost:40012")
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetMode(mgo.Eventual, false)

	c.Assert(session.Mode(), Equals, mgo.Eventual)

	result := M{}
	err = session.Run("ismaster", &result)
	c.Assert(err, IsNil)
	c.Assert(result["ismaster"], Equals, false)

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	result = M{}
	err = session.Run("ismaster", &result)
	c.Assert(err, IsNil)
	c.Assert(result["ismaster"], Equals, false)

	// Wait since the sync also uses sockets.
	for len(session.GetLiveServers()) != 3 {
		c.Log("Waiting for cluster sync to finish...")
		time.Sleep(5e8)
	}

	stats := mgo.GetStats()
	c.Assert(stats.MasterConns, Equals, 1)
	c.Assert(stats.SlaveConns, Equals, 2)
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestSetModeEventualAfterStrong(c *C) {
	// Test that a strong session shifting to an eventual
	// one preserves the socket untouched.

	session, err := mgo.Mongo("localhost:40012")
	c.Assert(err, IsNil)
	defer session.Close()

	// Insert something to force a connection to the master.
	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	session.SetMode(mgo.Eventual, false)

	// Wait since the sync also uses sockets.
	for len(session.GetLiveServers()) != 3 {
		c.Log("Waiting for cluster sync to finish...")
		time.Sleep(5e8)
	}

	// Master socket should still be reserved.
	stats := mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 1)

	// Confirm it's the master even though it's Eventual by now.
	result := M{}
	cmd := session.DB("admin").C("$cmd")
	err = cmd.Find(M{"ismaster": 1}).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result["ismaster"], Equals, true)

	session.SetMode(mgo.Eventual, true)

	stats = mgo.GetStats()
	c.Assert(stats.SocketsInUse, Equals, 0)
}

func (s *S) TestPrimaryShutdownStrong(c *C) {
	if *fast {
		c.Skip("-fast")
	}

	session, err := mgo.Mongo("localhost:40021")
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

	session.Refresh()

	// Now we should be able to talk to the new master.
	err = session.Run("serverStatus", result)
	c.Assert(err, IsNil)
	c.Assert(result.Host, Not(Equals), host)
}

func (s *S) TestPrimaryShutdownMonotonic(c *C) {
	if *fast {
		c.Skip("-fast")
	}

	session, err := mgo.Mongo("localhost:40021")
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

	// Insert something to force a switch to the master.
	coll := session.DB("mydb").C("mycoll")
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

	session.Refresh()

	// Now we should be able to talk to the new master.
	err = session.Run("serverStatus", result)
	c.Assert(err, IsNil)
	c.Assert(result.Host, Not(Equals), host)
}

func (s *S) TestPrimaryShutdownMonotonicWithSlave(c *C) {
	if *fast {
		c.Skip("-fast")
	}

	session, err := mgo.Mongo("localhost:40021")
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

	session, err = mgo.Mongo(addr)
	c.Assert(err, IsNil)
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)

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
	coll := session.DB("mydb").C("mycoll")
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

	session, err := mgo.Mongo("localhost:40021")
	c.Assert(err, IsNil)
	defer session.Close()

	result := &struct{ Host string }{}
	err = session.Run("serverStatus", result)
	c.Assert(err, IsNil)
	master := result.Host

	session.SetMode(mgo.Eventual, true)

	// Should connect to the master when needed.
	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"a": 1})
	c.Assert(err, IsNil)

	// Kill the master.
	s.Stop(master)

	// Should still work, with the new master now.
	coll = session.DB("mydb").C("mycoll")
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

	session, err := mgo.Mongo("localhost:40011")
	c.Assert(err, IsNil)
	defer session.Close()

	stats := mgo.GetStats()
	for stats.MasterConns+stats.SlaveConns != 3 {
		stats = mgo.GetStats()
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
		stats = mgo.GetStats()
		if stats.SocketsAlive == 3 {
			break
		}
		c.Logf("Waiting for 3 sockets alive, have %d", stats.SocketsAlive)
		time.Sleep(5e8)
	}

	// Ensure the number of sockets is preserved after syncing.
	stats = mgo.GetStats()
	c.Assert(stats.SocketsAlive, Equals, 3)
	c.Assert(stats.SocketsInUse, Equals, 1)
	c.Assert(stats.SocketRefs, Equals, 1)
}

func (s *S) TestSyncTimeout(c *C) {
	if *fast {
		c.Skip("-fast")
	}

	// 40009 isn't used by the test servers.
	session, err := mgo.Mongo("localhost:40009")
	c.Assert(err, IsNil)
	defer session.Close()

	timeout := int64(3e9)

	session.SetSyncTimeout(timeout)

	started := time.Nanoseconds()

	// Do something.
	result := struct{ Ok bool }{}
	err = session.Run("getLastError", &result)
	c.Assert(err, Matches, "no reachable servers")
	c.Assert(time.Nanoseconds()-started > timeout, Equals, true)
	c.Assert(time.Nanoseconds()-started < timeout*2, Equals, true)
}

func (s *S) TestDirect(c *C) {
	session, err := mgo.Mongo("localhost:40012?connect=direct")
	c.Assert(err, IsNil)
	defer session.Close()

	// We know that server is a slave.
	session.SetMode(mgo.Monotonic, true)

	result := &struct{ Host string }{}
	err = session.Run("serverStatus", result)
	c.Assert(err, IsNil)
	c.Assert(strings.HasSuffix(result.Host, ":40012"), Equals, true)

	stats := mgo.GetStats()
	c.Assert(stats.SocketsAlive, Equals, 1)
	c.Assert(stats.SocketsInUse, Equals, 1)
	c.Assert(stats.SocketRefs, Equals, 1)

	// We've got no master, so it'll timeout.
	session.SetSyncTimeout(5e8)

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"test": 1})
	c.Assert(err, Matches, "no reachable servers")

	// Slave is still reachable.
	result.Host = ""
	err = session.Run("serverStatus", result)
	c.Assert(err, IsNil)
	c.Assert(strings.HasSuffix(result.Host, ":40012"), Equals, true)
}

type OpCounters struct {
	Insert  int
	Query   int
	Update  int
	Delete  int
	GetMore int
	Command int
}

func getOpCounters(server string) (c *OpCounters, err os.Error) {
	session, err := mgo.Mongo(server + "?connect=direct")
	if err != nil {
		return nil, err
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	result := struct{ OpCounters }{}
	err = session.Run("serverStatus", &result)
	return &result.OpCounters, err
}

func (s *S) TestMonotonicSlaveOkFlagWithMongos(c *C) {
	session, err := mgo.Mongo("localhost:40021")
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

	// Collect op counters for everyone.
	opc21a, err := getOpCounters("localhost:40021")
	c.Assert(err, IsNil)
	opc22a, err := getOpCounters("localhost:40022")
	c.Assert(err, IsNil)
	opc23a, err := getOpCounters("localhost:40023")
	c.Assert(err, IsNil)

	// Do a SlaveOk query through MongoS

	mongos, err := mgo.Mongo("localhost:40202")
	c.Assert(err, IsNil)
	defer mongos.Close()

	mongos.SetMode(mgo.Monotonic, true)

	coll := mongos.DB("mydb").C("mycoll")
	result := &struct{}{}
	for i := 0; i != 5; i++ {
		err := coll.Find(nil).One(result)
		c.Assert(err, Equals, mgo.NotFound)
	}

	// Collect op counters for everyone again.
	opc21b, err := getOpCounters("localhost:40021")
	c.Assert(err, IsNil)
	opc22b, err := getOpCounters("localhost:40022")
	c.Assert(err, IsNil)
	opc23b, err := getOpCounters("localhost:40023")
	c.Assert(err, IsNil)

	masterPort := master[strings.Index(master, ":")+1:]

	var masterDelta, slaveDelta int
	switch masterPort {
	case "40021":
		masterDelta = opc21b.Query - opc21a.Query
		slaveDelta = (opc22b.Query - opc22a.Query) + (opc23b.Query - opc23a.Query)
	case "40022":
		masterDelta = opc22b.Query - opc22a.Query
		slaveDelta = (opc21b.Query - opc21a.Query) + (opc23b.Query - opc23a.Query)
	case "40023":
		masterDelta = opc23b.Query - opc23a.Query
		slaveDelta = (opc21b.Query - opc21a.Query) + (opc22b.Query - opc22a.Query)
	default:
		c.Fatal("Uh?")
	}

	c.Check(masterDelta, Equals, 0) // Just the counting itself.
	c.Check(slaveDelta, Equals, 5)  // The counting for both, plus 5 queries above.
}

func (s *S) TestAuthLogin(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")

	admindb := session.DB("admin")

	err = admindb.Login("root", "wrong")
	c.Assert(err, Matches, "auth fails")

	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginLogout(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	admindb.Logout()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")

	// Must have dropped auth from the session too.
	session = session.Copy()
	defer session.Close()

	coll = session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")
}

func (s *S) TestAuthLoginLogoutAll(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session.LogoutAll()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")

	// Must have dropped auth from the session too.
	session = session.Copy()
	defer session.Close()

	coll = session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")
}

func (s *S) TestAuthAddUser(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myruser", "mypass", true)
	c.Assert(err, IsNil)
	err = mydb.AddUser("mywuser", "mypass", false)
	c.Assert(err, IsNil)

	err = mydb.Login("myruser", "mypass")
	c.Assert(err, IsNil)

	admindb.Logout()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")

	err = mydb.Login("mywuser", "mypass")
	c.Assert(err, IsNil)

	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthAddUserReplaces(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "myoldpass", false)
	c.Assert(err, IsNil)
	err = mydb.AddUser("myuser", "mynewpass", true)
	c.Assert(err, IsNil)

	admindb.Logout()

	err = mydb.Login("myuser", "myoldpass")
	c.Assert(err, Matches, "auth fails")
	err = mydb.Login("myuser", "mynewpass")
	c.Assert(err, IsNil)

	// ReadOnly flag was changed too.
	err = mydb.C("mycoll").Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")
}

func (s *S) TestAuthRemoveUser(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "mypass", true)
	c.Assert(err, IsNil)
	err = mydb.RemoveUser("myuser")
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mypass")
	c.Assert(err, Matches, "auth fails")
}

func (s *S) TestAuthLoginTwiceDoesNothing(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	oldStats := mgo.GetStats()

	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)
}

func (s *S) TestAuthLoginLogoutLoginDoesNothing(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	oldStats := mgo.GetStats()

	admindb.Logout()
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)
}

func (s *S) TestAuthLoginSwitchUser(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)

	err = admindb.Login("reader", "rapadura")
	c.Assert(err, IsNil)

	// Can't write.
	err = coll.Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")

	// But can read.
	result := struct{ N int }{}
	err = coll.Find(nil).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 1)
}

func (s *S) TestAuthLoginChangePassword(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "myoldpass", false)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "myoldpass")
	c.Assert(err, IsNil)

	err = mydb.AddUser("myuser", "mynewpass", true)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mynewpass")
	c.Assert(err, IsNil)

	admindb.Logout()

	// The second login must be in effect, which means read-only.
	err = mydb.C("mycoll").Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")
}

func (s *S) TestAuthLoginCachingWithSessionRefresh(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session.Refresh()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingWithSessionCopy(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session = session.Copy()
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingWithSessionClone(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session = session.Clone()
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingWithNewSession(c *C) {
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session = session.New()
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")
}

func (s *S) TestAuthLoginCachingAcrossPool(c *C) {
	// Logins are cached even when the conenction goes back
	// into the pool.

	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	// Add another user to test the logout case at the same time.
	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "mypass", false)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Logout root explicitly, to test both cases.
	admindb.Logout()

	// Give socket back to pool.
	session.Refresh()

	// Brand new session, should use socket from the pool.
	other := session.New()
	defer other.Close()

	oldStats := mgo.GetStats()

	err = other.DB("admin").Login("root", "rapadura")
	c.Assert(err, IsNil)
	err = other.DB("mydb").Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Both logins were cached, so no ops.
	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)

	// And they actually worked.
	err = other.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)

	other.DB("admin").Logout()

	err = other.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingAcrossPoolWithLogout(c *C) {
	// Now verify that logouts are properly flushed if they
	// are not revalidated after leaving the pool.

	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	// Add another user to test the logout case at the same time.
	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "mypass", true)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Just some data to query later.
	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)

	// Give socket back to pool.
	session.Refresh()

	// Brand new session, should use socket from the pool.
	other := session.New()
	defer other.Close()

	oldStats := mgo.GetStats()

	err = other.DB("mydb").Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Login was cached, so no ops.
	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)

	// Can't write, since root has been implicitly logged out
	// when the collection went into the pool, and not revalidated.
	err = other.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, Matches, "unauthorized")

	// But can read due to the revalidated myuser login.
	result := struct{ N int }{}
	err = other.DB("mydb").C("mycoll").Find(nil).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 1)
}

func (s *S) TestAuthEventual(c *C) {
	// Eventual sessions don't keep sockets around, so they are
	// an interesting test case.
	session, err := mgo.Mongo("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(20)

	result := struct{ N int }{}
	for i := 0; i != 10; i++ {
		go func() {
			defer wg.Done()
			err = session.DB("mydb").C("mycoll").Find(nil).One(&result)
			c.Assert(err, IsNil)
			c.Assert(result.N, Equals, 1)
		}()
	}

	for i := 0; i != 10; i++ {
		go func() {
			defer wg.Done()
			err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
			c.Assert(err, IsNil)
		}()
	}

	wg.Wait()
}

func (s *S) TestAuthURL(c *C) {
	session, err := mgo.Mongo("mongodb://root:rapadura@localhost:40002/")
	c.Assert(err, IsNil)
	defer session.Close()

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthURLWrongCredentials(c *C) {
	session, err := mgo.Mongo("mongodb://root:wrong@localhost:40002/")
	c.Assert(err, IsNil)
	defer session.Close()

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, Matches, "auth fails")
}

func (s *S) TestAuthURLWithNewSession(c *C) {
	// When authentication is in the URL, the new session will
	// actually carry it on as well, even if logged out explicitly.
	session, err := mgo.Mongo("mongodb://root:rapadura@localhost:40002/")
	c.Assert(err, IsNil)
	defer session.Close()

	session.DB("admin").Logout()

	// Do it twice to ensure it passes the needed data on.
	session = session.New()
	defer session.Close()
	session = session.New()
	defer session.Close()

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestInt64Conversion(c *C) {
	session, err := mgo.Mongo("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()
	coll := session.DB("mydb").C("mycollection")
	result := struct { A int64 }{128}
	coll.Insert(&result)
	result.A = 0
	err = coll.Find(nil).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.A, Equals, int64(128))
}
