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

package mgo

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"launchpad.net/gobson/bson"
	"sync"
	"os"
	"reflect"
	"runtime"
	"strings"
	"time"
)

type mode int

const (
	Eventual mode = 0
	Monotonic mode = 1
	Strong mode = 2
)

// When changing the Session type, check if newSession and copySession
// need to be updated too.

type Session struct {
	m              sync.RWMutex
	cluster_       *mongoCluster
	socket         *mongoSocket
	socketIsMaster bool
	slaveOk        bool
	consistency    mode
	queryConfig    query
	safeOp         *queryOp
	syncTimeout    int64
	urlauth        *authInfo
	auth           []authInfo
}

type Database struct {
	Session *Session
	Name    string
}

type Collection struct {
	DB	Database
	Name    string  // "collection"
	FullName string // "db.collection"
}

type Query struct {
	m       sync.Mutex
	session *Session
	query   // Enables default settings in session.
}

type query struct {
	op       queryOp
	prefetch float64
	limit    int32
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
	limit          int32
	pendingDocs    int
	docsBeforeMore int
	timeout        int
}

var NotFound = os.ErrorString("Document not found")
var TailTimeout = os.ErrorString("Tail timed out")

const defaultPrefetch = 0.25


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
//     [mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]
//
// For example, it may be as simple as:
//
//     localhost
//
// Or more involved like:
//
//     mongodb://myuser:mypass@localhost:40001,otherhost:40001/mydb
//
// If the port number is not provided for a server, it defaults to 27017.
//
// The username and password provided in the URL will be used to authenticate
// into the database named after the slash at the end of the host names, or
// into the "admin" database if none is provided.  The authentication information
// will persist in sessions obtained through the New method as well.
//
// The following connection options are supported after the question mark:
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
	servers, auth, options, err := parseURL(url)
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
	if auth.user != "" {
		session.urlauth = &auth
		session.auth = []authInfo{auth}
	}
	cluster.Release()
	return session, nil
}

func parseURL(url string) (servers []string, auth authInfo, options map[string]string, err os.Error) {
	if strings.HasPrefix(url, "mongodb://") {
		url = url[10:]
	}
	options = make(map[string]string)
	if c := strings.Index(url, "?"); c != -1 {
		for _, pair := range strings.Split(url[c+1:], ";", -1) {
			l := strings.Split(pair, "=", 2)
			if len(l) != 2 || l[0] == "" || l[1] == "" {
				err = os.ErrorString("Connection option must be key=value: " + pair)
				return
			}
			options[l[0]] = l[1]
		}
		url = url[:c]
	}
	if c := strings.Index(url, "@"); c != -1 {
		pair := strings.Split(url[:c], ":", 2)
		if len(pair) != 2 || pair[0] == "" {
			err = os.ErrorString("Credentials must be provided as user:pass@host") 
			return
		}
		auth.user = pair[0]
		auth.pass = pair[1]
		url = url[c+1:]
		auth.db = "admin"
	}
	if c := strings.Index(url, "/"); c != -1 {
		if c != len(url) - 1 {
			auth.db = url[c+1:]
		}
		url = url[:c]
	}
	if auth.user == "" {
		if auth.db != "" {
			err = os.ErrorString("Database name only makes sense with credentials")
			return
		}
	} else if auth.db == "" {
		auth.db = "admin"
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


func newSession(consistency mode, cluster *mongoCluster, socket *mongoSocket) (session *Session) {
	cluster.Acquire()
	session = &Session{cluster_: cluster}
	session.SetMode(consistency, true)
	session.SetSafe(&Safe{})
	session.setSocket(socket)
	session.queryConfig.prefetch = defaultPrefetch
	runtime.SetFinalizer(session, finalizeSession)
	return session
}

func copySession(session *Session, keepAuth bool) (s *Session) {
	session.cluster().Acquire()
	if session.socket != nil {
		session.socket.Acquire()
	}
	var auth []authInfo
	if keepAuth {
		auth = make([]authInfo, len(session.auth))
		copy(auth, session.auth)
	} else if session.urlauth != nil {
		auth = []authInfo{*session.urlauth}
	}
	// Copy everything but the mutex.
	s = &Session{
		cluster_:       session.cluster_,
		socket:         session.socket,
		socketIsMaster: session.socketIsMaster,
		slaveOk:        session.slaveOk,
		consistency:    session.consistency,
		queryConfig:    session.queryConfig,
		safeOp:         session.safeOp,
		syncTimeout:    session.syncTimeout,
		urlauth:        session.urlauth,
		auth:		auth,
	}
	runtime.SetFinalizer(s, finalizeSession)
	return s
}

func finalizeSession(session *Session) {
	session.Close()
}

// GetLiveServers returns a list of server addresses which are
// currently known to be alive.
func (session *Session) GetLiveServers() (addrs []string) {
	session.m.RLock()
	addrs = session.cluster().GetLiveServers()
	session.m.RUnlock()
	return addrs
}

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
	return Collection{database, name, database.Name + "." + name}
}

// GridFS returns a GridFS value for interacting with collections in the
// database which follow the standard GridFS specification.  The provided
// prefix (sometimes known as root) will determine which collections to
// use, and is usually set to "fs" when there is a single GridFS in the
// database.
//
// See the GridFS Create, Open, and OpenId methods for more details.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/GridFS
//     http://www.mongodb.org/display/DOCS/GridFS+Tools
//     http://www.mongodb.org/display/DOCS/GridFS+Specification
//
func (database Database) GridFS(prefix string) *GridFS {
	return newGridFS(database, prefix)
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

// Login authenticates against MongoDB with the provided credentials.  The
// authentication is valid for the whole session and will stay valid until
// Logout is explicitly called for the same database, or the session is
// closed.
//
// Concurrent Login calls will work correctly.
func (database Database) Login(user, pass string) (err os.Error) {
	session := database.Session
	dbname := database.Name

	socket, err := session.acquireSocket(false)
	if err != nil {
		return err
	}
	defer socket.Release()

	err = socket.Login(dbname, user, pass)
	if err != nil {
		return err
	}

	session.m.Lock()
	defer session.m.Unlock()

	for _, a := range session.auth {
		if a.db == dbname {
			a.user = user
			a.pass = pass
			return nil
		}
	}
	session.auth = append(session.auth, authInfo{dbname, user, pass})
	return nil
}

// Logout removes any established authentication credentials for the database.
func (database Database) Logout() {
	session := database.Session
	dbname := database.Name
	session.m.Lock()
	found := false
	for i, a := range session.auth {
		if a.db == dbname {
			copy(session.auth[i:], session.auth[i+1:])
			session.auth = session.auth[:len(session.auth)-1]
			found = true
			break
		}
	}
	if found && session.socket != nil {
		session.socket.Logout(dbname)
	}
	session.m.Unlock()
}

// LogoutAll removes all established authentication credentials for the session.
func (session *Session) LogoutAll() {
	session.m.Lock()
	for _, a := range session.auth {
		session.socket.Logout(a.db)
	}
	session.auth = session.auth[0:0]
	session.m.Unlock()
}

// AddUser creates or updates the authentication credentials of user within
// the database.
func (database Database) AddUser(user, pass string, readOnly bool) os.Error {
	psum := md5.New()
	psum.Write([]byte(user + ":mongo:" + pass))
	digest := hex.EncodeToString(psum.Sum())
	c := database.C("system.users")
	return c.Upsert(bson.M{"user": user}, bson.M{"$set": bson.M{"user": user, "pwd": digest, "readOnly": readOnly}})
}

// RemoveUser removes the authentication credentials of user from the database.
func (database Database) RemoveUser(user string) os.Error {
	c := database.C("system.users")
	return c.Remove(bson.M{"user": user})
}

type indexSpec struct {
	Name, NS string
	Key bson.D
	Unique bool "/c"
	DropDups bool "dropDups/c"
	Background bool "/c"
	Sparse bool "/c"
	Bits, Min, Max int "/c"
}

type Index struct {
	Key []string       // Index key fields; prefix name with dash (-) for descending order
	Unique bool        // Prevent two documents from having the same index key
	DropDups bool      // Drop documents with the same index key as a previously indexed one
	Background bool    // Build index in background and return immediately
	Sparse bool        // Only index documents containing the Key fields

	Name string        // Index name, computed by EnsureIndex

	Bits, Min, Max int // Properties for spatial indexes
}

func parseIndexKey(key []string) (name string, realKey bson.D, err os.Error) {
	var order interface{}
	for _, field := range key {
		if name != "" {
			name += "_"
		}
		if field != "" {
			switch field[0] {
			case '@':
				order = "2d"
				field = field[1:]
				name += field + "_" // Why don't they put 2d here?
			case '-':
				order = -1
				field = field[1:]
				name += field + "_-1"
			case '+':
				field = field[1:]
				fallthrough
			default:
				order = 1
				name += field + "_1"
			}
		}
		if field == "" {
			return "", nil, os.ErrorString("Invalid index key: empty field name")
		}
		realKey = append(realKey, bson.DocElem{field, order})
	}
	if name == "" {
		return "", nil, os.ErrorString("Invalid index key: no fields provided")
	}
	return
}

// EnsureIndexKey ensures an index with the given key exists, creating it
// if necessary.
//
// This example:
//
//     err := collection.EnsureIndexKey([]string{"a", "b"})
//
// Is equivalent to:
//
//     err := collection.EnsureIndex(mgo.Index{Key: []string{"a", "b"}})
//
// See the EnsureIndex method for more details.
func (collection Collection) EnsureIndexKey(key []string) os.Error {
    return collection.EnsureIndex(Index{Key: key})
}

// EnsureIndex ensures an index with the given key exists, creating it with
// the provided parameters if necessary.
//
// Once EnsureIndex returns successfully, following requests for the same index
// will not contact the server unless Collection.DropIndex is used to drop the
// same index, or Session.ResetIndexCache is called.
//
// For example:
//
//     index := Index{
//         Key: []string{"lastname", "firstname"},
//         Unique: true,
//         DropDups: true,
//         Background: true,
//         Sparse: true,
//     }
//     err := collection.EnsureIndex(index)
//
// The Key value determines which fields compose the index. The index ordering
// will be ascending by default.  To obtain an index with a descending order,
// the field name should be prefixed by a dash (e.g. []string{"-time"}).
//
// If Unique is true, the index must necessarily contain only a single
// document per Key.  With DropDups set to true, documents with the same key
// as a previously indexed one will be dropped rather than an error returned.
//
// If Background is true, the operation will return immediately and will
// continue in background.  The index won't be used for queries until the build
// is complete.
//
// If Sparse is true, only documents containing the provided Key fields will be
// included in the index.  When using a sparse index for sorting, only indexed
// documents will be returned.
//
// Spatial indexes are also supported through that API.  Here is an example:
//
//     index := Index{
//         Key: []string{"@loc"},
//         Bits: 26,
//     }
//     err := collection.EnsureIndex(index)
//
// The "@" prefix in the field name will request the creation of a "2d" index
// for the given field.
//
// The 2D index bounds may be changed using the Min and Max attributes of the
// Index value.  The default bound setting of (-180, 180) is suitable for
// latitude/longitude pairs.
//
// The Bits parameter sets the precision of the 2D geohash values.  If not
// provided, 26 bits are used, which is roughly equivalent to 1 foot of
// precision for the default (-180, 180) index bounds.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Indexes
//     http://www.mongodb.org/display/DOCS/Indexing+Advice+and+FAQ
//     http://www.mongodb.org/display/DOCS/Indexing+as+a+Background+Operation
//     http://www.mongodb.org/display/DOCS/Geospatial+Indexing
//     http://www.mongodb.org/display/DOCS/Multikeys
//
func (collection Collection) EnsureIndex(index Index) os.Error {
	name, realKey, err := parseIndexKey(index.Key)
	if err != nil {
		return err
	}

	db := collection.DB
	session := db.Session
	cacheKey := collection.FullName + "\x00" +  name
	if session.cluster().HasCachedIndex(cacheKey) {
		return nil
	}

	spec := indexSpec{
		Name: name,
		NS: collection.FullName,
		Key: realKey,
		Unique: index.Unique,
		DropDups: index.DropDups,
		Background: index.Background,
		Sparse: index.Sparse,
		Bits: index.Bits,
		Min: index.Min,
		Max: index.Max,
	}

	session = session.Clone()
	defer session.Close()
	session.SetMode(Strong, false)
	session.EnsureSafe(&Safe{})

	db.Session = session
	err = db.C("system.indexes").Insert(&spec)
	if err == nil {
		session.cluster().CacheIndex(cacheKey, true)
	}
	return err
}

// DropIndex removes the index with key from the collection.
//
// The key value determines which fields compose the index. The index ordering
// will be ascending by default.  To obtain an index with a descending order,
// the field name should be prefixed by a dash (e.g. []string{"-time"}).
//
// For example:
//
//     err := collection.DropIndex([]string{"lastname", "firstname"})
//
// See the EnsureIndex method for more details on indexes.
func (collection Collection) DropIndex(key []string) os.Error {
	name, _, err := parseIndexKey(key)
	if err != nil {
		return err
	}

	db := collection.DB
	session := db.Session
	cacheKey := collection.FullName + "\x00" +  name
	session.cluster().CacheIndex(cacheKey, false)

	session = session.Clone()
	defer session.Close()
	session.SetMode(Strong, false)

	db.Session = session
	defer db.Session.Close()
	result := struct{ ErrMsg string; Ok bool }{}
	err = db.Run(bson.D{{"dropIndexes", collection.Name}, {"index", name}}, &result)
	if err != nil {
		return err
	}
	if !result.Ok {
		return os.ErrorString(result.ErrMsg)
	}
	return nil
}

// Indexes returns a list of all indexes for the collection.
//
// For example, this snippet would drop all available indexes:
//
//   indexes, err := collection.Indexes()
//   if err != nil {
//       panic(err)
//   }
//   for _, index := range indexes {
//       err = collection.DropIndex(index.Key)
//       if err != nil {
//           panic(err)
//       }
//   }
//
// See the EnsureIndex method for more details on indexes.
func (collection Collection) Indexes() (indexes []Index, err os.Error) {
	query := collection.DB.C("system.indexes").Find(bson.M{"ns": collection.FullName})
	iter, err := query.Sort(bson.D{{"name", 1}}).Iter()
	for {
		var spec indexSpec
		err = iter.Next(&spec)
		if err != nil {
			break
		}
		index := Index{
			Name: spec.Name,
			Key: simpleIndexKey(spec.Key),
			Unique: spec.Unique,
			DropDups: spec.DropDups,
			Background: spec.Background,
			Sparse: spec.Sparse,
		}
		indexes = append(indexes, index)
	}
	if err == NotFound {
		err = nil
	}
	return
}

func simpleIndexKey(realKey bson.D) (key []string) {
	for i := range realKey {
		field := realKey[i].Name
		i, _ := realKey[i].Value.(int)
		if i == 1 {
			key = append(key, field)
			continue
		}
		if i == -1 {
			key = append(key, "-" + field)
			continue
		}
		s, _ := realKey[i].Value.(string)
		if s == "2d" {
			key = append(key, "@" + field)
			continue
		}
		panic("Got unknown index key type for field " + field)
	}
	return
}

// ResetIndexCache() clears the cache of previously ensured indexes.
// Following requests to EnsureIndex will contact the server.
func (session *Session) ResetIndexCache() {
	session.cluster().ResetIndexCache()
}

// New creates a new session with the same parameters as the original
// session, including consistency, batch size, prefetching, safety mode,
// etc. The returned session will use sockets from the poll, so there's
// a chance that writes just performed in another session may not yet
// be visible.
//
// Login information from the original session will not be copied over
// into the new session unless it was provided through the initial URL
// for the Mongo function.
//
// See the Copy and Clone methods.
//
func (session *Session) New() *Session {
	session.m.Lock()
	s := copySession(session, false)
	session.m.Unlock()
	s.Refresh()
	return s
}

// Copy works just like New, but preserves the exact authentication
// information from the original session.
func (session *Session) Copy() *Session {
	session.m.Lock()
	s := copySession(session, true)
	session.m.Unlock()
	s.Refresh()
	return s
}

// Clone works just like Copy, but also reuses the same socket as the original
// session, in case it had already reserved one due to its consistency
// guarantees.  This behavior ensures that writes performed in the old session
// are necessarily observed when using the new session, as long as it was a
// strong or monotonic session.  That said, it also means that long operations
// may cause other goroutines using the original session to wait.
func (session *Session) Clone() *Session {
	session.m.Lock()
	s := copySession(session, true)
	session.m.Unlock()
	return s
}

// Close terminates the session.  It's a runtime error to use a session
// after it has been closed.
func (session *Session) Close() {
	session.m.Lock()
	if session.cluster_ != nil {
		session.setSocket(nil)
		session.cluster_.Release()
		session.cluster_ = nil
	}
	session.m.Unlock()
}

func (session *Session) cluster() *mongoCluster {
	if session.cluster_ == nil {
		panic("Session already closed")
	}
	return session.cluster_
}

// Refresh puts back any reserved sockets in use and restarts the consistency
// guarantees according to the current consistency setting for the session.
func (session *Session) Refresh() {
	session.m.Lock()
	session.slaveOk = session.consistency != Strong
	session.setSocket(nil)
	session.m.Unlock()
}

// SetMode changes the consistency mode for the session.
//
// In the Strong consistency mode reads and writes will always be made to
// the master server using a unique connection so that reads and writes are
// fully consistent, ordered, and observing the most up-to-date data.
// This offers the least benefits in terms of distributing load, but the
// most guarantees.  See also Monotonic and Eventual.
//
// In the Monotonic consistency mode reads may not be entirely up-to-date,
// but they will always see the history of changes moving forward, the data
// read will be consistent across sequential queries in the same session,
// and modifications made within the session will be observed in following
// queries (read-your-writes).
//
// In practice, the Monotonic mode is obtained by performing initial reads
// against a unique connection to an arbitrary slave, if one is available,
// and once the first write happens, the session connection is switched over
// to the master server.  This manages to distribute some of the reading
// load with slaves, while maintaining some useful guarantees.
//
// In the Eventual consistency mode reads will be made to any slave in the
// cluster, if one is available, and sequential reads will not necessarily
// be made with the same connection.  This means that data may be observed
// out of order.  Writes will of course be issued to the master, but
// independent writes in the same Eventual session may also be made with
// independent connections, so there are also no guarantees in terms of
// write ordering (no read-your-writes guarantees either).
//
// The Eventual mode is the fastest and most resource-friendly, but is
// also the one offering the least guarantees about ordering of the data
// read and written.
//
// If refresh is true, in addition to ensuring the session is in the given
// consistency mode, the consistency guarantees will also be reset (e.g.
// a Monotonic session will be allowed to read from slaves again).  This is
// equivalent to calling the Refresh function.
//
// Shifting between Monotonic and Strong modes will keep a previously
// reserved connection for the session unless refresh is true or the
// connection is unsuitable (to a slave server in a Strong session).
func (session *Session) SetMode(consistency mode, refresh bool) {
	session.m.Lock()
	debugf("Session %p: setting mode %d with refresh=%v (socket=%p)", session, consistency, refresh, session.socket)
	session.consistency = consistency
	if refresh {
		session.slaveOk = session.consistency != Strong
		session.setSocket(nil)
	} else if session.consistency == Strong {
		session.slaveOk = false
	} else if session.socket == nil {
		session.slaveOk = true
	}
	session.m.Unlock()
}

// Mode returns the current consistency mode for the session.
func (session *Session) Mode() mode {
	session.m.RLock()
	mode := session.consistency
	session.m.RUnlock()
	return mode
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

// SetBatch sets the default batch size used when fetching documents from the
// database. It's possible to change this setting on a per-query basis as
// well, using the Query.Batch method.
//
// The default batch size is defined by the database itself.  As of this
// writing, MongoDB will use an initial size of min(100 docs, 4MB) on the
// first batch, and 4MB on remaining ones.
func (session *Session) SetBatch(n int) {
	session.m.Lock()
	session.queryConfig.op.limit = int32(n)
	session.m.Unlock()
}

// SetPrefetch sets the default point at which the next batch of results will be
// requested.  When there are p*batch_size remaining documents cached in an
// Iter, the next batch will be requested in background. For instance, when
// using this:
//
//     session.SetBatch(200)
//     session.SetPrefetch(0.25)
//
// and there are only 50 documents cached in the Iter to be processed, the
// next batch of 200 will be requested. It's possible to change this setting on
// a per-query basis as well, using the Prefetch method of Query.
//
// The default prefetch value is 0.25.
func (session *Session) SetPrefetch(p float64) {
	session.m.Lock()
	session.queryConfig.prefetch = p
	session.m.Unlock()
}

type Safe struct {
	W int        // Min # of servers that have to ack before success
	WTimeout int // Milliseconds to wait for W before timing out
	FSync bool   // Should servers sync to disk before returning success
}

// Safe returns the current safety mode for the session.
func (session *Session) Safe() (safe *Safe) {
	session.m.Lock()
	defer session.m.Unlock()
	if session.safeOp != nil {
		cmd := session.safeOp.query.(*getLastError)
		safe = &Safe{cmd.W, cmd.WTimeout, cmd.FSync}
	}
	return
}

// SetSafe changes the session safety mode.
//
// If the safe parameter is nil, the session is put in unsafe mode, and writes
// become fire-and-forget, without error checking.  The unsafe mode is faster
// since operations won't hold on waiting for a confirmation.
//
// If the safe parameter is not nil, any changing query (insert, update, ...)
// will be followed by a getLastError command with the specified parameters,
// to ensure the request was correctly processed.
//
// The safe.W parameter determines how many servers should confirm a write
// before the operation is considered successful.  If set to 0 or 1, the
// command will return as soon as the master is done with the request.
// If safe.WTimeout is greater than zero, it determines how many milliseconds
// to wait for the safe.W servers to respond before returning an error.
// If safe.FSync is true, servers will synchronize the change to disk before
// confirming its success.
//
// For example, the following statement will make the session check for
// errors, without imposing further constraints:
//
//     session.SetSafe(&mgo.Safe{})
//
// The following statement, on the other hand, ensures that at least two
// servers have flushed the change to disk before confirming the success
// of operations:
//
//     session.EnsureSafe(&mgo.Safe{W: 2, FSync: true})
//
// The following statement, on the other hand, disables the verification
// of errors entirely:
//
//     session.SetSafe(nil)
//
// See also the EnsureSafe method.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Last+Error+Commands
//     http://www.mongodb.org/display/DOCS/Verifying+Propagation+of+Writes+with+getLastError
//
func (session *Session) SetSafe(safe *Safe) {
	session.m.Lock()
	session.safeOp = nil
	session.ensureSafe(safe)
	session.m.Unlock()
}

// EnsureSafe compares the provided safety parameters with the ones
// currently in use by the session and merges the most conservative choices
// to be used in the session.  That is, if the provided safe.W is larger
// than the one in use, the session will use it.  If safe.FSync is true,
// it will necessarily be set in the session.  If safe.WTimeout is not
// zero and is less than the value currently in the session, the session
// value will be changed to that.
//
// For example, the following statement will ensure the session is
// at least checking for errors, without enforcing further constraints.
// If a more conservative SetSafe or EnsureSafe call was previously done,
// the following call will be ignored.
//
//     session.EnsureSafe(&mgo.Safe{})
//
// See also the SetSafe method.
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Last+Error+Commands
//     http://www.mongodb.org/display/DOCS/Verifying+Propagation+of+Writes+with+getLastError
//
func (session *Session) EnsureSafe(safe *Safe) {
	session.m.Lock()
	session.ensureSafe(safe)
	session.m.Unlock()
}

func (session *Session) ensureSafe(safe *Safe) {
	if safe == nil {
		return
	}

	var cmd getLastError
	if session.safeOp == nil {
		cmd = getLastError{1, safe.W, safe.WTimeout, safe.FSync}
	} else {
		// Copy.  We don't want to mutate the existing query.
		cmd = *(session.safeOp.query.(*getLastError))
		if safe.W > cmd.W {
			cmd.W = safe.W
		}
		if safe.WTimeout > 0 && safe.WTimeout < cmd.WTimeout {
			cmd.WTimeout = safe.WTimeout
		}
		if safe.FSync {
			cmd.FSync = true
		}
	}
	session.safeOp = &queryOp{
		query:      &cmd,
		collection: "admin.$cmd",
		limit:      -1,
	}
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

// Ping runs a trivial ping command just to get in touch with the server.
func (session *Session) Ping() os.Error {
	result := struct{}{} // We don't care.
	return session.Run("ping", &result)
}

// Find prepares a query using the provided document.  The document may be a
// map or a struct value capable of being marshalled with bson.  The map
// may be a generic one using interface{} for its key and/or values, such as
// bson.M, or it may be a properly typed map.  Providing nil as the document
// is equivalent to providing an empty document such as bson.M{}.
//
// Further details of the query may be tweaked using the resulting Query value,
// and then executed using methods such as One, Iter, or Tail.
//
// In case the resulting document includes a field named $err, which is a
// standard way for MongoDB to return query errors, the returned err will be
// set to a *QueryError value including the Err message and the Code.  In those
// cases, the result argument is still unmarshalled into with the received
// document so that any other custom values may be obtained if desired.
//
// For example:
//
//     err := collection.Find(bson.M{"a", 1}).One(result)
//
// Or, for iterating over all results:
//
//    iter, err := collection.Find(nil).Iter()
//    if err != nil {
//        panic(err)
//    }
//    for {
//        err = iter.Next(&result)
//        if err != nil {
//            break
//        }
//        fmt.Println(result.Id)
//    }
//    if err != nil {
//        panic(err)
//    }
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
	session := collection.DB.Session
	session.m.RLock()
	q := &Query{session: session, query: session.queryConfig}
	session.m.RUnlock()
	q.op.query = query
	q.op.collection = collection.FullName
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

type QueryError struct {
	Err string "$err"
	Code int
}

func (err *QueryError) String() string {
	return err.Err
}

// Insert inserts one or more documents in the respective collection.  In
// case the session is in safe mode (see the SetSafe method) and an error
// happens while inserting the provided documents, the returned error will
// be of type *LastError.
func (collection Collection) Insert(docs ...interface{}) os.Error {
	return collection.DB.Session.writeQuery(&insertOp{collection.FullName, docs})
}

// Update finds a single document matching the provided selector document
// and modifies it according to the change document.  In case the session
// is in safe mode (see the SetSafe method) and an error happens when attempting
// the change, the returned error will be of type *LastError.
func (collection Collection) Update(selector interface{}, change interface{}) os.Error {
	return collection.DB.Session.writeQuery(&updateOp{collection.FullName, selector, change, 0})
}

// Upsert finds a single document matching the provided selector document
// and modifies it according to the change document.  If no document matching
// the selector is found, the change document is newly inserted instead.
// In case the session is in safe mode (see the SetSafe method) and an error
// happens when attempting the change, the returned error will be of type
// *LastError.
func (collection Collection) Upsert(selector interface{}, change interface{}) os.Error {
	return collection.DB.Session.writeQuery(&updateOp{collection.FullName, selector, change, 1})
}

// UpdateAll finds all documents matching the provided selector document
// and modifies them according to the change document.  In case the session
// is in safe mode (see the SetSafe method) and an error happens when attempting
// the change, the returned error will be of type *LastError.
func (collection Collection) UpdateAll(selector interface{}, change interface{}) os.Error {
	return collection.DB.Session.writeQuery(&updateOp{collection.FullName, selector, change, 2})
}

// Remove finds a single document matching the provided selector document
// and removes it from the database.  In case the session is in safe mode
// (see the SetSafe method) and an error happens when attempting the change,
// the returned error will be of type *LastError.
func (collection Collection) Remove(selector interface{}) os.Error {
	return collection.DB.Session.writeQuery(&deleteOp{collection.FullName, selector, 1})
}

// RemoveAll finds all documents matching the provided selector document
// and removes them from the database.  In case the session is in safe mode
// (see the SetSafe method) and an error happens when attempting the change,
// the returned error will be of type *LastError.
func (collection Collection) RemoveAll(selector interface{}) os.Error {
	return collection.DB.Session.writeQuery(&deleteOp{collection.FullName, selector, 0})
}

// Batch sets the batch size used when fetching documents from the database.
// It's possible to change this setting on a per-session basis as well, using
// the Batch method of Session.
//
// The default batch size is defined by the database itself.  As of this
// writing, MongoDB will use an initial size of min(100 docs, 4MB) on the
// first batch, and 4MB on remaining ones.
func (query *Query) Batch(n int) *Query {
	query.m.Lock()
	query.op.limit = int32(n)
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
// a per-session basis as well, using the SetPrefetch method of Session.
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

// Limit restricts the maximum number of documents retrieved to n, and also
// changes the batch size to the same value.  Once n documents have been
// returned by Next, the following call will return NotFound.
func (query *Query) Limit(n int) *Query {
	query.m.Lock()
	query.limit = int32(n)
	query.op.limit = -int32(n)
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
	Hint interface{} "$hint/c"
	Explain bool "$explain/c"
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
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Sorting+and+Natural+Order
//
func (query *Query) Sort(order interface{}) *Query {
	query.m.Lock()
	w := query.wrap()
	w.OrderBy = order
	query.m.Unlock()
	return query
}

// Explain returns a number of details about how the MongoDB server would
// execute the requested query, such as the number of objects examined,
// the number of time the read lock was yielded to allow writes to go in,
// and so on.
//
// For example:
//
//     m := bson.M{}
//     err := collection.Find(bson.M{"filename": name}).Explain(m)
//     if err == nil {
//         fmt.Printf("Explain: %#v\n", m)
//     }
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Optimization
//     http://www.mongodb.org/display/DOCS/Query+Optimizer
//     
func (query *Query) Explain(result interface{}) os.Error {
	query.m.Lock()
	clone := &Query{session: query.session, query: query.query}
	query.m.Unlock()
	w := clone.wrap()
	w.Explain = true
	if clone.op.limit > 0 {
		clone.op.limit = -query.op.limit
	}
	iter, err := clone.Iter()
	if err != nil {
		return err
	}
	return iter.Next(result)
}

// Hint will include an explicit "hint" in the query to force the server
// to use a specified index, potentially improving performance in some
// situations.  The indexKey parameter must be set to the key of the
// index to be used.  For details on how the indexKey may be built, see
// the EnsureIndex method.
//
// For example:
//
//     query := collection.Find(bson.M{"a": 4, "b": 5, "c": 6}).Hint([]string{"a", "b"})
//
// Relevant documentation:
//
//     http://www.mongodb.org/display/DOCS/Optimization
//     http://www.mongodb.org/display/DOCS/Query+Optimizer
//     
func (query *Query) Hint(indexKey []string) *Query {
	query.m.Lock()
	_, realKey, err := parseIndexKey(indexKey)
	w := query.wrap()
	w.Hint = realKey
	query.m.Unlock()
	if err != nil {
		panic(err)
	}
	return query
}

var errHint = []byte("\x02$err\x00")

func checkQueryError(data []byte) os.Error {
	if bytes.Index(data, errHint) < 0 {
		return nil
	}
	result := &QueryError{}
	err := bson.Unmarshal(data, result)
	if err == nil && result.Err != "" {
		return result
	}
	return nil
}

// One executes the query and unmarshals the first obtained document into the
// result argument.  The result must be a struct or map value capable of being
// unmarshalled into by gobson.  This function blocks until either a result
// is available or an error happens.  For example:
//
//     err := collection.Find(bson.M{"a", 1}).One(&result)
//
// In case the resulting document includes a field named $err, which is a
// standard way for MongoDB to return query errors, the returned err will be
// set to a *QueryError value including the Err message and the Code.  In those
// cases, the result argument is still unmarshalled into with the received
// document so that any other custom values may be obtained if desired.
//
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

	op.flags |= session.slaveOkFlag()
	op.limit = -1

	data, err := socket.SimpleQuery(&op)
	if err != nil {
		return err
	}
	if data == nil {
		return NotFound
	}

	err = bson.Unmarshal(data, result)
	if err == nil {
		debugf("Query %p document unmarshaled: %#v", query, result)
	} else {
		debugf("Query %p document unmarshaling failed: %#v", query, err)
		return err
	}

	return checkQueryError(data)
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
	limit := query.limit
	query.m.Unlock()

	socket, err := session.acquireSocket(true)
	if err != nil {
		return nil, err
	}
	defer socket.Release()

	iter = &Iter{session: session, prefetch: prefetch, limit: limit}
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
//         for {
//             err := iter.Next(&result)
//             if err == mgo.TailTimeout {
//                 continue
//             }
//             if err != nil {
//                 break
//             }
//             fmt.Println(result.Id)
//             lastId = result.Id
//         }
//         if err != mgo.NotFound {
//             panic(err)
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
// if pre-fetching is enabled (see the Query.Prefetch and Session.SetPrefetch
// methods).
//
// Next returns NotFound at the end of the result set, or in case a tailable
// iterator becomes invalid, and returns TailTimeout if a tailable iterator
// times out (see the Tail method of Query).  Also, in case the resulting
// document includes a field named $err, which is a standard way for MongoDB
// to return query errors, the returned err will be set to a *QueryError value
// including the Err message and the Code.  In those cases, the result argument
// is still unmarshalled into with the received document so that any other
// custom values may be obtained if desired.
//
// For example:
//
//    iter := collection.Find(nil).Iter()
//    for {
//        err := iter.Next(&result)
//        if err != nil {
//            break
//        }
//        println(result.Id)
//    }
//    if err != mgo.NotFound {
//        panic(err)
//    }
//
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
		iter.limit--
		if iter.limit == 0 {
			// XXX Must kill the cursor here.
			iter.err = NotFound
		}
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
		return checkQueryError(docData)
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

// The For method unmarshals into result each document found through an
// iterator obtained from query and calls f to handle it.  The result
// value must necessarily be a pointer to a nil reference type.
// If f returns a non-nil os.Error, iteration will stop and the error
// will be returned as the result of For.
//
// For example:
//
//     var result *struct{ N int }
//     err := collection.Find(nil).For(&result, func() os.Error {
//         println(result.N)
//         return nil
//     })
//     if err != nil {
//         panic(err)
//     }
//
// Note the way in which result is declared.  The following are also valid
// declaration examples to be used with For.  With all of these, the variable
// must still be passed by address (&result) in the For call.
//
//     var result *MyType
//     var result bson.M
//     var result map[string]interface{}
//
func (query *Query) For(result interface{}, f func() os.Error) (err os.Error) {
	iter, err := query.Iter()
	if err != nil {
		return err
	}
	return iter.For(result, f)
}

// The For method unmarshals into result each document found through iter
// and calls f to handle it.  The result value must necessarily be a
// pointer to a nil reference type.  If f returns a non-nil os.Error,
// iteration will stop and the error will be returned as the result of For.
//
// For example:
//
//     var result *struct{ N int }
//     err := iter.For(&result, func() os.Error {
//         println(result.N)
//         return nil
//     })
//     if err != nil {
//         panic(err)
//     }
//
// Note the way in which result is declared.  The following are also valid
// declaration examples to be used with For.  With all of these, the variable
// must still be passed by address (&result) in the For call.
//
//     var result *MyType
//     var result bson.M
//     var result map[string]interface{}
//
func (iter *Iter) For(result interface{}, f func() os.Error) (err os.Error) {
	valid := false
	v := reflect.ValueOf(result)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		switch v.Kind() {
		case reflect.Map, reflect.Ptr, reflect.Interface, reflect.Slice:
			valid = v.IsNil()
		}
	}
	if !valid {
		panic("For needs a pointer to nil reference value.  See the documentation.")
	}
	zero := reflect.Zero(v.Type())
	for err == nil {
		v.Set(zero)
		err = iter.Next(result)
		if err == nil {
			err = f()
		}
	}
	if err == NotFound {
		return nil
	}
	return err
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
	if iter.limit > 0 && iter.op.limit > iter.limit {
		iter.op.limit = -iter.limit
	}
	err = socket.Query(&iter.op)
	if err != nil {
		iter.err = err
	}
}

type countCmd struct {
	Count string
	Query interface{}
}

// Count returns the total number of documents in the result set.
func (query *Query) Count() (n int, err os.Error) {
	query.m.Lock()
	session := query.session
	op := query.op
	query.m.Unlock()

	c := strings.Index(op.collection, ".")
	if c < 0 {
		return 0, os.ErrorString("Bad collection name: " + op.collection)
	}

	dbname := op.collection[:c]
	cname := op.collection[c+1:]

	q := op.query
	if qw, ok := q.(*queryWrapper); ok {
		q = qw.Query
	}

	result := struct{ N int }{}
	err = session.DB(dbname).Run(countCmd{cname, q}, &result)
	return result.N, err
}

// Count returns the total number of documents in the collection.
func (collection Collection) Count() (n int, err os.Error) {
	return collection.Find(nil).Count()
}


// ---------------------------------------------------------------------------
// Internal session handling helpers.

func (session *Session) acquireSocket(slaveOk bool) (s *mongoSocket, err os.Error) {

	// Try to use a previously reserved socket, with a fast read-only lock.
	session.m.RLock()
	s = session.socket
	sIsGood := s != nil && (slaveOk && session.slaveOk || session.socketIsMaster)
	session.m.RUnlock()

	if sIsGood {
		s.Acquire()
		return s, nil
	}

	// No go.  We may have to request a new socket and change the session,
	// so try again but with an exclusive lock now.
	session.m.Lock()
	defer session.m.Unlock()

	s = session.socket
	sIsGood = s != nil && (slaveOk && session.slaveOk || session.socketIsMaster)

	if sIsGood {
		s.Acquire()
		return s, nil
	}

	// Still not good.  We need a new socket.
	s, err = session.cluster().AcquireSocket(slaveOk && session.slaveOk, session.syncTimeout)
	if err != nil {
		return nil, err
	}

	// Authenticate the new socket.
	for _, a := range session.auth {
		err = s.Login(a.db, a.user, a.pass)
		if err != nil {
			s.Release()
			return nil, err
		}
	}

	// Keep track of the new socket, if necessary.
	// Note that, as a special case, if the Eventual session was
	// not refreshed (socket != nil), it means the developer asked
	// to preserve an existing reserved socket, so we'll keep the
	// master one around too before a Refresh happens.
	if session.consistency != Eventual || session.socket != nil {
		session.setSocket(s)
	}

	// Switch over a Monotonic session to the master.
	if !slaveOk && session.consistency == Monotonic {
		session.slaveOk = false
	}

	return s, nil
}

// Set the socket bound to this session.  With a bound socket, all operations
// with this session will use the given socket if possible. When not possible
// (e.g. attempting to write to a slave) acquireSocket will replace the
// current socket.  Note that this method will properly refcount the socket up
// and down when setting/releasing.
func (session *Session) setSocket(socket *mongoSocket) {
	if socket != nil {
		session.socketIsMaster = socket.Acquire()
	} else {
		session.socketIsMaster = false
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

// writeQuery runs the given modifying operation, potentially followed up
// by a getLastError command in case the session is in safe mode.
func (session *Session) writeQuery(op interface{}) os.Error {
	socket, err := session.acquireSocket(false)
	if err != nil {
		return err
	}
	defer socket.Release()

	session.m.RLock()
	safeOp := session.safeOp
	session.m.RUnlock()

	if safeOp == nil {
		return socket.Query(op)
	} else {
		var mutex sync.Mutex
		var replyData []byte
		var replyErr os.Error
		mutex.Lock()
		query := *safeOp // Copy the data.
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
