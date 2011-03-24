/*
mgo - MongoDB driver for Go

Copyright (c) 2010-2011 - Gustavo Niemeyer <gustavo@niemeyer.net>

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its
      contributors may be used to endorse or promote products derived from
      this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package mgo

import (
	"launchpad.net/gobson/bson"
	"sync"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	Strong = iota
	Monotonic
	Eventual
)

type Session struct {
	m           sync.RWMutex
	cluster     *mongoCluster
	socket      *mongoSocket
	queryConfig query
	safe        *queryOp
	syncTimeout int64
	consistency int
	slaveOk     bool
}

type Database struct {
	Session *Session
	Name    string
}

type Collection struct {
	Session *Session
	Name    string
}

type Query struct {
	m       sync.Mutex
	session *Session
	query   // Enables default settings in session.
}

type query struct {
	op       queryOp
	prefetch float64
}

type getLastError struct {
	CmdName  int  "getLastError"
	W        int  "w/c"
	WTimeout int  "wtimeout/c"
	FSync    bool "fsync/c"
}

type Iter struct {
	m              sync.Mutex
	gotReply       sync.Cond
	session        *Session
	docData        queue
	err            os.Error
	op             getMoreOp
	prefetch       float64
	pendingDocs    int
	docsBeforeMore int
	timeout        int
}

var NotFound = os.ErrorString("Document not found")
var TailTimeout = os.ErrorString("Tail timed out")

const defaultPrefetch = 0.25


// ---------------------------------------------------------------------------
// Entry point function to the cluster/session/server/socket hierarchy.

// Mongo establishes a new session to the cluster identified by the given seed
// server(s).  The session will enable communication with all of the servers in
// the cluster, so the seed servers are used only to find out about the cluster
// topology.
//
// This method is generally called just once for a given cluster.  Further
// sessions to the same cluster are then established using the New method on
// the obtained session.  This will make them share the underlying cluster,
// and manage the pool of connections appropriately.
//
// Once the session is not useful anymore, Close must be called to release the
// resources appropriately.
//
// The seed servers must be provided in the following format:
//
//     [mongodb://]host1[:port1][,host2[:port2],...][?options]
//
// If the port number is not provided for a server, it defaults to 27017.
//
// The following options are supported:
//
//     connect=direct
//
//         This option will disable the automatic replica set server
//         discovery logic, and will only use the servers provided.
//         This enables forcing the communication with a specific
//         server or set of servers (even if they are slaves).  Note
//         that to talk to a slave you'll need to relax the consistency
//         requirements via the Monotonic or Eventual session methods.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Connections
//
func Mongo(url string) (session *Session, err os.Error) {
	servers, options, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	direct := false
	for k, v := range options {
		switch k {
		case "connect":
			if v == "direct" {
				direct = true
			} else if v != "replicaSet" {
				goto bad
			}
		default:
bad:
			err = os.ErrorString("Unsupported connection URL option: " + k + "=" + v)
			return
		}
	}
	cluster := newCluster(servers, direct)
	session = newSession(Strong, cluster, nil)
	cluster.Release()
	return session, nil
}

func parseURL(url string) (servers []string, options map[string]string, err os.Error) {
	if strings.Contains(url, "@") {
		err = os.ErrorString("Authentication not supported in URL yet; coming soon")
		return
	}
	if strings.HasPrefix(url, "mongodb://") {
		url = url[10:]
	}
	if c := strings.Index(url, "?"); c != -1 {
		options = make(map[string]string)
		for _, pair := range strings.Split(url[c+1:], ";", -1) {
			l := strings.Split(pair, "=", 2)
			if len(l) != 2 || l[0] == "" || l[1] == "" {
				err = os.ErrorString("Connection option must be key=value: " + pair)
			}
			options[l[0]] = l[1]
		}
		url = url[:c]
	}
	if c := strings.Index(url, "/"); c != -1 {
		if c != len(url) - 1 {
			err = os.ErrorString("Database name not supported in URL yet; coming soon")
		}
		url = url[:c]
	}
	servers = strings.Split(url, ",", -1)
	// XXX This is untested. The test suite doesn't use the standard port.
	for i, server := range servers {
		p := strings.LastIndexAny(server, "]:")
		if p == -1 || server[p] != ':' {
			servers[i] = server + ":27017"
		}
	}
	return
}


// ---------------------------------------------------------------------------
// Public session methods.

// DB returns a database object, which allows further accessing any
// collections within it, or performing any database-level operations.
// Creating this object is a very lightweight operation, and involves
// no network communication.
func (session *Session) DB(name string) Database {
	return Database{session, name}
}

// C returns a collection object for the given database, which enables querying
// and modifying the content contained in the given collection. Creating this
// object is a very lightweight operation, and involves no network
// communication.
func (database Database) C(name string) Collection {
	return Collection{database.Session, database.Name + "." + name}
}

// Run issues the provided command against the database and unmarshals
// its result in the respective argument. The cmd argument may be either
// a string with the command name itself, in which case an empty document of
// the form bson.M{cmd: 1} will be used, or it may be a full command document.
//
// Note that MongoDB considers the first marshalled key as the command
// name, so when providing a command with options, it's important to
// use an ordering-preserving document, such as a struct value or an
// instance of bson.D.  For instance:
//
//     db.Run(mgo.D{{"create", "mycollection"}, {"size", 1024}})
//
// For privilleged commands typically run against the "admin" database, see
// the Run method in the Session type.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Commands
//     http://www.mongodb.org/display/DOCS/List+of+Database+CommandSkips
//
func (database Database) Run(cmd interface{}, result interface{}) os.Error {
	if name, ok := cmd.(string); ok {
		cmd = bson.M{name: 1}
	}
	return database.C("$cmd").Find(cmd).One(result)
}

// New creates a new session with the same parameters as the original
// session, including consistency, batch size, prefetching, safety mode,
// etc. The new session will not share any sockets with the old session
// (see the Clone method for a different behavior).
func (session *Session) New() *Session {
	session.m.Lock()
	s := &Session{
		cluster:     session.cluster,
		safe:        session.safe,
		queryConfig: session.queryConfig,
		consistency: session.consistency,
		slaveOk:     session.consistency != Strong,
	}
	session.cluster.Acquire()
	session.m.Unlock()
	return s
}

// Clone creates a new session with the same parameters as the current one,
// including consistency, batch size, prefetching, safety mode, etc. In case
// a socket has already been reserved by the original session to preserve
// consistency requirements, the same socket will be shared with the new
// session (for a different behavior see the New method).
func (session *Session) Clone() *Session {
	session.m.Lock()
	s := &Session{
		consistency: session.consistency,
		cluster:     session.cluster,
		safe:        session.safe,
		queryConfig: session.queryConfig,
		slaveOk:     session.slaveOk,
	}
	session.cluster.Acquire()
	s.setSocket(session.socket)
	session.m.Unlock()
	return s
}

// Close terminates the session.  It's a runtime error to use a session
// after it has been closed.
func (session *Session) Close() {
	session.m.Lock()
	if session.cluster != nil {
		session.setSocket(nil)
		session.cluster.Release()
		session.cluster = nil
	}
	session.m.Unlock()
}

// Restart puts back any reserved sockets in use and restarts the consistency
// guarantees according to the existing consistency setting.
func (session *Session) Restart() {
	session.restart(-1)
}

// Strong restarts the session in strong consistency mode.
//
// In this mode, reads and writes will always be made to the master server
// using a unique connection so that reads and writes are fully consistent,
// ordered, and observing the most up-to-date data.
//
// This offers the least benefits in terms of distributing load, but the
// most guarantees.  See also Monotonic and Eventual.
func (session *Session) Strong() {
	session.restart(Strong)
}

// Monotonic restarts the session in monotonic consistency mode.
//
// In this mode, reads may not be entirely up-to-date, but they will always
// see the history of changes moving forward, the data read will be consistent
// across sequential queries in the same session, and modifications made within
// the session will be observed in following queries (read-your-writes).
//
// In practice, this consistency level is obtained by performing initial reads
// against a unique connection to an arbitrary slave, if one is available, and
// once the first write happens, the session connection is switched over
// to the master server.
//
// This manages to distribute some of the reading load with slaves, while
// maintaining some useful guarantees.  See also Strong and Eventual.
func (session *Session) Monotonic() {
	session.restart(Monotonic)
}

// Eventual restarts the session in eventual consistency mode.
//
// In this mode, reads will be made to any slave in the cluster, if one is
// available, and sequential reads will not necessarily be made with the same
// connection.  This means that data may be observed out of order.
//
// Writes will of course be issued to the master, but independent writes in
// the same session may also be made with independent connections, so there
// are also no guarantees in terms of write ordering, let alone reading your
// own writes.
//
// This mode is the fastest and most resource-friendly, but is also the one
// offering the least guarantees about ordering of the data read and written.
// See also Strong and Monotonic.
func (session *Session) Eventual() {
	session.restart(Eventual)
}

func (session *Session) restart(consistency int) {
	session.m.Lock()
	if consistency == -1 {
		consistency = session.consistency
	} else {
		session.consistency = consistency
	}
	session.slaveOk = consistency != Strong
	session.setSocket(nil)
	session.m.Unlock()
}

// SetSyncTimeout sets the amount of time an operation with this session
// will wait before returning an error in case a connection to a usable
// server can't be established. Set it to zero to wait forever. This is
// the default.
func (session *Session) SetSyncTimeout(nsec int64) {
	session.m.Lock()
	session.syncTimeout = nsec
	session.m.Unlock()
}

// Batch sets the default batch size used when fetching documents from the
// database. It's possible to change this setting on a per-query basis as
// well, using the Batch method of Query.
//
// The default batch size is defined by the database itself.  As of this
// writing, MongoDB will use an initial size of min(100 docs, 4MB) on the
// first batch, and 4MB on remaining ones.
func (session *Session) Batch(size int) {
	session.m.Lock()
	session.queryConfig.op.limit = int32(size)
	session.m.Unlock()
}

// Prefetch sets the default point at which the next batch of results will be
// requested.  When there are p*batch_size remaining documents cached in an
// Iter, the next batch will be requested in background. For instance, when
// using this:
//
//     session.Batch(200)
//     session.Prefetch(0.25)
//
// and there are only 50 documents cached in the Iter to be processed, the
// next batch of 200 will be requested. It's possible to change this setting on
// a per-query basis as well, using the Prefetch method of Query.
//
// The default prefetch value is 0.25.
func (session *Session) Prefetch(p float64) {
	session.m.Lock()
	session.queryConfig.prefetch = p
	session.m.Unlock()
}

// Unsafe puts the session in unsafe mode. Writes will become fire-and-forget,
// without error checking.  The unsafe mode is faster since operations won't
// hold on waiting for a confirmation.  It's also unsafe, though! ;-)  In
// addition to disabling it entirely, the parameters of safety can also be
// tweaked via the Safe() method.  It's also possible to modify the safety
// settings on a per-query basis, using the Safe and Unsafe methods of Query.
func (session *Session) Unsafe() {
	session.m.Lock()
	session.safe = nil
	session.m.Unlock()
}

// Safe puts the session into safe mode.  Once in safe mode, any changing
// query (insert, update, ...) will be followed by a getLastError command
// with the specified parameters, to ensure the request was correctly
// processed.
func (session *Session) Safe(w, wtimeout int, fsync bool) {
	session.m.Lock()
	session.safe = &queryOp{
		query:      &getLastError{1, w, wtimeout, fsync},
		collection: "admin.$cmd",
		limit:      -1,
	}
	session.m.Unlock()
}

// Run issues the provided command against the "admin" database and
// and unmarshals its result in the respective argument. The cmd
// argument may be either a string with the command name itself, in
// which case an empty document of the form bson.M{cmd: 1} will be used,
// or it may be a full command document.
//
// Note that MongoDB considers the first marshalled key as the command
// name, so when providing a command with options, it's important to
// use an ordering-preserving document, such as a struct value or an
// instance of bson.D.  For instance:
//
//     db.Run(bson.D{{"create", "mycollection"}, {"size", 1024}})
//
// For commands against arbitrary databases, see the Run method in
// the Database type.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Commands
//     http://www.mongodb.org/display/DOCS/List+of+Database+CommandSkips
//
func (session *Session) Run(cmd interface{}, result interface{}) os.Error {
	return session.DB("admin").Run(cmd, result)
}

// Find prepares a query using the provided document.  The document may be a
// map or a struct value capable of being marshalled with bson.  The map
// may be a generic one using interface{} for its key and/or values, such as
// bson.M, or it may be a properly typed map.  Providing nil as the document
// is equivalent to providing an empty document such as bson.M{}.
//
// Further details of the query may be tweaked using the resulting Query value,
// and then executed using methods such as One, Iter, or Tail.  For example:
//
//     err := collection.Find(bson.M{"a", 1}).One(&result)
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Querying
//     http://www.mongodb.org/display/DOCS/Advanced+Queries
//
func (collection Collection) Find(query interface{}) *Query {
	if query == nil {
		query = bson.M{}
	}
	session := collection.Session
	q := &Query{session: session, query: session.queryConfig}
	q.op.query = query
	q.op.collection = collection.Name
	return q
}

type LastError struct {
	Err             string
	Code, N, Waited int
	WTimeout        bool
	FSyncFiles      int "fsyncFiles"
}

func (err *LastError) String() string {
	return err.Err
}

// Insert inserts one or more documents in the respective collection.  In
// case the session is in safe mode (see the Safe method) and an error
// happens while inserting the provided documents, the returned error will
// be of type *LastError.
func (collection Collection) Insert(docs ...interface{}) os.Error {
	return collection.Session.writeQuery(&insertOp{collection.Name, docs})
}

// Update finds a single document matching the provided selector document
// and modifies it according to the change document.  In case the session
// is in safe mode (see the Safe method) and an error happens when attempting
// the change, the returned error will be of type *LastError.
func (collection Collection) Update(selector interface{}, change interface{}) os.Error {
	return collection.Session.writeQuery(&updateOp{collection.Name, selector, change, 0})
}

// Upsert finds a single document matching the provided selector document
// and modifies it according to the change document.  If no document matching
// the selector is found, the change document is newly inserted instead.
// In case the session is in safe mode (see the Safe method) and an error
// happens when attempting the change, the returned error will be of type
// *LastError.
func (collection Collection) Upsert(selector interface{}, change interface{}) os.Error {
	return collection.Session.writeQuery(&updateOp{collection.Name, selector, change, 1})
}

// UpdateAll finds all documents matching the provided selector document
// and modifies them according to the change document.  In case the session
// is in safe mode (see the Safe method) and an error happens when attempting
// the change, the returned error will be of type *LastError.
func (collection Collection) UpdateAll(selector interface{}, change interface{}) os.Error {
	return collection.Session.writeQuery(&updateOp{collection.Name, selector, change, 2})
}

// Remove finds a single document matching the provided selector document
// and removes it from the database.  In case the session is in safe mode
// (see the Safe method) and an error happens when attempting the change,
// the returned error will be of type *LastError.
func (collection Collection) Remove(selector interface{}) os.Error {
	return collection.Session.writeQuery(&deleteOp{collection.Name, selector, 1})
}

// RemoveAll finds all documents matching the provided selector document
// and removes them from the database.  In case the session is in safe mode
// (see the Safe method) and an error happens when attempting the change,
// the returned error will be of type *LastError.
func (collection Collection) RemoveAll(selector interface{}) os.Error {
	return collection.Session.writeQuery(&deleteOp{collection.Name, selector, 0})
}

// Batch sets the batch size used when fetching documents from the database.
// It's possible to change this setting on a per-session basis as well, using
// the Batch method of Session.
//
// The default batch size is defined by the database itself.  As of this
// writing, MongoDB will use an initial size of min(100 docs, 4MB) on the
// first batch, and 4MB on remaining ones.
func (query *Query) Batch(size int) *Query {
	query.m.Lock()
	query.op.limit = int32(size)
	query.m.Unlock()
	return query
}

// Prefetch sets the point at which the next batch of results will be requested.
// When there are p*batch_size remaining documents cached in an Iter, the next
// batch will be requested in background. For instance, when using this:
//
//     query.Batch(200).Prefetch(0.25)
//
// and there are only 50 documents cached in the Iter to be processed, the
// next batch of 200 will be requested. It's possible to change this setting on
// a per-session basis as well, using the Prefetch method of Session.
//
// The default prefetch value is 0.25.
func (query *Query) Prefetch(p float64) *Query {
	query.m.Lock()
	query.prefetch = p
	query.m.Unlock()
	return query
}

// Skip skips over the n initial documents from the query results.  Note that
// this only makes sense with capped collections where documents are naturally
// ordered by insertion time, or with sorted results.
func (query *Query) Skip(n int) *Query {
	query.m.Lock()
	query.op.skip = int32(n)
	query.m.Unlock()
	return query
}

// Select enables selecting which fields should be retrieved for the results
// found. For example, the following query would only retrieve the name field:
//
//     err := collection.Find(nil).Select(bson.M{"name": 1}).One(&result)
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Retrieving+a+Subset+of+Fields
//
func (query *Query) Select(selector interface{}) *Query {
	query.m.Lock()
	query.op.selector = selector
	query.m.Unlock()
	return query
}

type queryWrapper struct {
	Query   interface{} "$query"
	OrderBy interface{} "$orderby/c"
}

func (query *Query) wrap() *queryWrapper {
	w, ok := query.op.query.(*queryWrapper)
	if !ok {
		w = &queryWrapper{Query: query.op.query}
		query.op.query = w
	}
	return w
}

// Sort asks the database to order returned documents according to the rules
// provided in the given document.
func (query *Query) Sort(order interface{}) *Query {
	query.m.Lock()
	w := query.wrap()
	w.OrderBy = order
	query.m.Unlock()
	return query
}

// One executes the query and unmarshals the first obtained document into the
// result argument.  The result must be a struct or map value capable of being
// unmarshalled into by gobson.  This function blocks until either a result
// is available or an error happened. 
func (query *Query) One(result interface{}) (err os.Error) {
	query.m.Lock()
	session := query.session
	op := query.op // Copy.
	query.m.Unlock()

	socket, err := session.acquireSocket(true)
	if err != nil {
		return err
	}
	defer socket.Release()

	var mutex sync.Mutex
	var replyData []byte
	var replyErr os.Error

	mutex.Lock()

	op.flags |= session.slaveOkFlag()
	op.limit = -1
	op.replyFunc = func(err os.Error, reply *replyOp, docNum int, docData []byte) {
		replyErr = err
		replyData = docData
		mutex.Unlock()
	}

	err = socket.Query(&op)
	if err != nil {
		return err
	}

	mutex.Lock()
	if replyErr != nil {
		return replyErr
	}
	if replyData == nil {
		return NotFound
	}

	// Unmarshal outside of the read goroutine (replyFunc) to avoid blocking it.
	err = bson.Unmarshal(replyData, result)
	if err == nil {
		debugf("Query %p document unmarshaled: %#v", query, result)
	} else {
		debugf("Query %p document unmarshaling failed: %#v", query, err)
	}
	return err
}

// Iter executes the query and returns an iterator capable of going over all
// the results. Results will be returned in batches of configurable
// size (see the Batch method) and more documents will be requested when a
// configurable threshold is reached (see the Prefetch method).
func (query *Query) Iter() (iter *Iter, err os.Error) {
	query.m.Lock()
	session := query.session
	op := query.op
	prefetch := query.prefetch
	query.m.Unlock()

	socket, err := session.acquireSocket(true)
	if err != nil {
		return nil, err
	}
	defer socket.Release()

	iter = &Iter{session: session, prefetch: prefetch}
	iter.gotReply.L = &iter.m
	iter.op.collection = op.collection
	iter.op.limit = op.limit
	iter.op.replyFunc = iter.replyFunc()
	iter.pendingDocs++
	op.replyFunc = iter.op.replyFunc
	op.flags |= session.slaveOkFlag()

	err = socket.Query(&op)
	if err != nil {
		return nil, err
	}

	return iter, nil
}

// Tail returns a tailable iterator.  Unlike a normal iterator, a
// tailable iterator will wait for new values to be inserted in the
// collection once the end of the current result set is reached.
// A tailable iterator may only be used with capped collections.
//
// The timeoutSecs parameter indicates how long Next will block
// waiting for a result before returning TailTimeout.  If set to -1,
// Next will not timeout, and will continue waiting for a result
// for as long as the cursor is valid and the session is not closed.
// If set to 0, Next will return TailTimeout as soon as it reaches
// the end of the result set.  Otherwise, Next will wait for at
// least the given number of seconds for a new document to be
// available before aborting and returning TailTimeout.
//
// When Next returns TailTimeout, it may still be called again to
// check if a new value is available. If Next returns NotFound,
// though, it means the cursor became invalid, and the query must
// be restarted.
//
// This example demonstrates query restarting in case the cursor
// becomes invalid:
//
//    query := collection.Find(nil)
//    for {
//         iter := query.Sort("$natural").Tail(-1)
//         for iter.Next(&result) != mgo.NotFound {
//             println(result.Id)
//             lastId = result.Id
//         }
//         query = collection.Find(bson.M{"_id", bson.M{"$gt", lastId}})
//    }
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Tailable+Cursors
//     http://www.mongodb.org/display/DOCS/Capped+Collections
//     http://www.mongodb.org/display/DOCS/Sorting+and+Natural+Order
//
func (query *Query) Tail(timeoutSecs int) (iter *Iter, err os.Error) {
	query.m.Lock()
	session := query.session
	op := query.op
	prefetch := query.prefetch
	query.m.Unlock()

	socket, err := session.acquireSocket(true)
	if err != nil {
		return nil, err
	}
	defer socket.Release()

	iter = &Iter{session: session, prefetch: prefetch}
	iter.gotReply.L = &iter.m
	iter.timeout = timeoutSecs
	iter.op.collection = op.collection
	iter.op.limit = op.limit
	iter.op.replyFunc = iter.replyFunc()
	iter.pendingDocs++
	op.replyFunc = iter.op.replyFunc
	op.flags |= 2 | 32 | session.slaveOkFlag() // Tailable | AwaitData [| SlaveOk]

	err = socket.Query(&op)
	if err != nil {
		return nil, err
	}

	return iter, nil
}

func (session *Session) slaveOkFlag() (flag uint32) {
	session.m.RLock()
	if session.slaveOk {
		flag = 4
	}
	session.m.RUnlock()
	return
}

// Next retrieves the next document from the result set, blocking if necessary.
// This method will also automatically retrieve another batch of documents from
// the server when the current one is exhausted, or before that in background
// if pre-fetching is enabled (see the Prefetch method).
//
// Next returns NotFound at the end of the result set, or in case a tailable
// iterator becomes invalid, and returns TailTimeout if a tailable iterator
// times out (see the Tail method of Query).
func (iter *Iter) Next(result interface{}) (err os.Error) {
	timeout := int64(-1)
	if iter.timeout >= 0 {
		timeout = time.Nanoseconds() + int64(iter.timeout)*1e9
	}

	iter.m.Lock()

	for iter.err == nil && iter.docData.Len() == 0 && (iter.pendingDocs > 0 || iter.op.cursorId != 0) {
		if iter.pendingDocs == 0 && iter.op.cursorId != 0 {
			// Tailable cursor exhausted.
			if timeout >= 0 && time.Nanoseconds() > timeout {
				iter.m.Unlock()
				return TailTimeout
			}
			iter.getMore()
		}
		iter.gotReply.Wait()
	}

	// Exhaust available data before returning any errors.
	if docData, ok := iter.docData.Pop().([]byte); ok {
		if iter.op.cursorId != 0 && iter.err == nil {
			iter.docsBeforeMore--
			if iter.docsBeforeMore == 0 {
				iter.getMore()
			}
		}
		iter.m.Unlock()
		err = bson.Unmarshal(docData, result)
		if err == nil {
			debugf("Iter %p document unmarshaled: %#v", iter, result)
		} else {
			debugf("Iter %p document unmarshaling failed: %#v", iter, err)
		}
		return err
	} else if iter.err != nil {
		err := iter.err
		debugf("Iter %p returning error: %s", iter, err)
		iter.m.Unlock()
		return err
	} else if iter.op.cursorId == 0 {
		debugf("Iter %p returning NotFound with cursor=0", iter)
		iter.m.Unlock()
		return NotFound
	}

	panic("Internal error: this should be unreachable")
	return
}


// ---------------------------------------------------------------------------
// Internal session handling helpers.

func newSession(consistency int, cluster *mongoCluster, socket *mongoSocket) (session *Session) {
	cluster.Acquire()
	session = &Session{consistency: consistency, cluster: cluster}
	session.setSocket(socket)
	session.queryConfig.prefetch = defaultPrefetch
	session.Safe(0, 0, false)
	runtime.SetFinalizer(session, finalizeSession)
	return session
}

func finalizeSession(session *Session) {
	session.Close()
}

func (session *Session) acquireSocket(slaveOk bool) (s *mongoSocket, err os.Error) {
	session.m.RLock()
	if session.consistency == Eventual {
		s, err = session.cluster.AcquireSocket(slaveOk, session.syncTimeout)
		session.m.RUnlock()
		return
	}

	s = session.socket
	if s == nil || !slaveOk && session.slaveOk {
		session.m.RUnlock()
		// Try again, with an exclusive lock now.
		session.m.Lock()
		s = session.socket
		if swap := !slaveOk && session.slaveOk; swap || s == nil {
			if swap {
				session.slaveOk = false
			}
			s, err = session.cluster.AcquireSocket(session.slaveOk, session.syncTimeout)
			if err == nil {
				session.setSocket(s)
			}
		}
		session.m.Unlock()
	} else {
		session.m.RUnlock()
		s.Acquire()
	}
	return
}

// Set the socket bound to this session.  With a bound socket, all operations
// with this session will use the given socket if possible. When not possible
// (e.g. attempting to write to a slave) acquireSocket will replace the
// current socket.  Note that this method will properly refcount the socket up
// and down when setting/releasing.
func (session *Session) setSocket(socket *mongoSocket) {
	if socket != nil {
		socket.Acquire() // Hold a reference while the session is using it.
	}
	if session.socket != nil {
		session.socket.Release()
	}
	session.socket = socket
}

func (iter *Iter) replyFunc() replyFunc {
	return func(err os.Error, op *replyOp, docNum int, docData []byte) {
		iter.m.Lock()
		iter.pendingDocs--
		if err != nil {
			iter.err = err
			debugf("Iter %p received an error: %s", iter, err.String())
		} else if docNum == -1 {
			debugf("Iter %p received no documents (cursor=%d).", iter, op.cursorId)
			if op != nil && op.cursorId != 0 {
				// It's a tailable cursor.
				iter.op.cursorId = op.cursorId
			} else {
				iter.err = NotFound
			}
		} else {
			rdocs := int(op.replyDocs)
			if docNum == 0 {
				iter.pendingDocs += rdocs - 1
				iter.docsBeforeMore = rdocs - int(iter.prefetch*float64(rdocs))
				iter.op.cursorId = op.cursorId
			}
			// XXX Handle errors and flags.
			debugf("Iter %p received reply document %d/%d", iter, docNum+1, rdocs)
			iter.docData.Push(docData)
		}
		iter.gotReply.Broadcast()
		iter.m.Unlock()
	}
}

func (iter *Iter) getMore() {
	socket, err := iter.session.acquireSocket(true)
	if err != nil {
		iter.err = err
		return
	}
	defer socket.Release()
	debugf("Iter %p requesting more documents", iter)
	iter.pendingDocs++
	err = socket.Query(&iter.op)
	if err != nil {
		iter.err = err
	}
}

// writeQuery runs the given modifying operation, potentially followed up
// by a getLastError command in case the session is in safe mode.
func (session *Session) writeQuery(op interface{}) os.Error {
	socket, err := session.acquireSocket(false)
	if err != nil {
		return err
	}
	defer socket.Release()

	// Copy safe's address to avoid locking.
	if safe := session.safe; safe == nil {
		return socket.Query(op)
	} else {
		var mutex sync.Mutex
		var replyData []byte
		var replyErr os.Error
		mutex.Lock()
		query := *safe // Copy the data.
		query.replyFunc = func(err os.Error, reply *replyOp, docNum int, docData []byte) {
			replyData = docData
			replyErr = err
			mutex.Unlock()
		}
		err = socket.Query(op, &query)
		if err != nil {
			return err
		}
		mutex.Lock() // Wait.
		if replyErr != nil {
			return replyErr // XXX TESTME
		}
		result := &LastError{}
		bson.Unmarshal(replyData, &result)
		debugf("Result from writing query: %#v", result)
		if result.Err != "" {
			return result
		}
	}
	return nil
}
