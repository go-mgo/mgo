package mongogo


import (
    "gobson"
    "sync"
    "os"
)


const (
    StrongConsistency = iota
    MonotonicConsistency
    EventualConsistency
)

type Session struct {
    sync.RWMutex
    consistency int
    cluster *mongoCluster
    socket *mongoSocket
}

type Database struct {
    Session *Session
    Name string
}

type Collection struct {
    Session *Session
    Name string
}

type Query struct {
    session *Session
    op queryOp
}

// ---------------------------------------------------------------------------
// Session handling.

func newSession(consistency int, cluster *mongoCluster, socket *mongoSocket) (
        session *Session) {
    session = &Session{consistency:consistency, cluster:cluster}
    session.setSocket(socket)
    return session
}

// Return a database object. This is a very lightweight operation, and
// involves no network communication.
func (session *Session) DB(name string) Database {
    return Database{session, name}
}

// Return a collection object for the given database. This is a very
// lightweight operation, and involves no network communication.
func (database Database) C(name string) Collection {
    return Collection{database.Session, database.Name + "." + name}
}

func (session *Session) getSocket(write bool) (s *mongoSocket, err os.Error) {
    // XXX Must take into account consistency setting.
    session.RLock()
    s = session.socket
    if s == nil || write && !s.server.Master {
        session.RUnlock()
        // Try again, with an exclusive lock now.
        session.Lock()
        s = session.socket
        if s == nil || write && !s.server.Master {
            s, err = session.cluster.AcquireSocket(write)
            if err == nil {
                session.setSocket(s)
            }
        }
        session.Unlock()
    } else {
        session.RUnlock()
    }
    return
}

func (session *Session) setSocket(socket *mongoSocket) {
    if session.socket != nil {
        session.socket.Recycle()
    }
    session.socket = socket
    if socket != nil {
        socket.Reserve()
    }
}

func (session *Session) Reset() {
    session.Lock()
    session.setSocket(nil)
    session.Unlock()
}


// ---------------------------------------------------------------------------
// Type management.

func (session *Session) RunM(cmd interface{}) (value gobson.M, err os.Error) {
    if name, ok := cmd.(string); ok {
        cmd = gobson.M{name: 1}
    }
    c := session.DB("admin").C("$cmd")
    return c.Find(cmd).OneM()
}

func (session *Session) Run(cmd interface{}, result interface{}) (err os.Error) {
    if name, ok := cmd.(string); ok {
        cmd = gobson.M{name: 1}
    }
    c := session.DB("admin").C("$cmd")
    return c.Find(cmd).One(result)
}


// ---------------------------------------------------------------------------
// Database, collection, and result operations.

func (collection Collection) Insert(documents ...interface{}) os.Error {
    socket, err := collection.Session.getSocket(true)
    if err != nil {
        return err
    }
    debugf("Socket: %#v", socket)
    err = socket.Query(&insertOp{collection.Name, documents})
    return err
}

func (collection Collection) Find(query interface{}) *Query {
    q := &Query{session:collection.Session}
    q.op.query = query
    q.op.collection = collection.Name
    return q
}

func (query *Query) OneM() (doc gobson.M, err os.Error) {
    m, err := query.one(nil)
    if err == nil {
         doc = m.(gobson.M)
    }
    return
}

func (query *Query) One(result interface{}) (err os.Error) {
    _, err = query.one(result)
    return err
}

func (query *Query) one(doc interface{}) (interface{}, os.Error) {

    socket, err := query.session.getSocket(false)
    if err != nil {
        return nil, err
    }

    var mutex sync.Mutex
    var docData []byte

    mutex.Lock()
    query.op.limit = -1
    query.op.replyFunc = func(reply *replyOp, docNum int, _docData []byte) {
        docData = _docData
        mutex.Unlock()
    }

    err = socket.Query(&query.op)
    if err != nil {
        return nil, err
    }

    // Unmarshal outside of the read goroutine to avoid blocking it.
    mutex.Lock()

    if doc == nil {
        doc = make(gobson.M)
    }
    err = gobson.UnmarshalTo(doc, docData)
    if err != nil {
        return nil, err
    }
    return doc, nil
}
