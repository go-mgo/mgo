package mgo


import (
    "sync"
    "sort"
    "net"
    "os"
)


// ---------------------------------------------------------------------------
// Mongo server encapsulation.

type mongoServer struct {
    sync.RWMutex
    Master       bool
    Addr         string
    ResolvedAddr string
    tcpaddr      *net.TCPAddr
    sockets      []*mongoSocket
    closed       bool
}


func newServer(addr string) (server *mongoServer, err os.Error) {
    tcpaddr, err := net.ResolveTCPAddr(addr)
    if err != nil {
        log("Failed to resolve ", addr, ": ", err.String())
        return nil, err
    }

    resolvedAddr := tcpaddr.String()
    if resolvedAddr != addr {
        debug("Address ", addr, " resolved as ", resolvedAddr)
    }
    server = &mongoServer{Addr: addr, ResolvedAddr: resolvedAddr, tcpaddr: tcpaddr}
    return
}


// Obtain a socket for communicating with the server.  This will attempt to
// reuse an old connection, if one is available. Otherwise, it will establish
// a new one. The returned socket is owned by the call site, and will return
// to the cache if explicitly done.
func (server *mongoServer) AcquireSocket() (socket *mongoSocket, err os.Error) {
    for {
        server.Lock()
        n := len(server.sockets)
        if n > 0 {
            socket = server.sockets[n-1]
            server.sockets[n-1] = nil // Help GC.
            server.sockets = server.sockets[:n-1]
            server.Unlock()
            err = socket.Acquired(server)
            if err != nil {
                continue
            }
        } else {
            server.Unlock()
            socket, err = server.Connect()
        }
        return
    }
    panic("unreached")
}

// Establish a new connection to the server. This should generally be done
// through server.getSocket().
func (server *mongoServer) Connect() (*mongoSocket, os.Error) {
    server.RLock()
    addr := server.Addr
    tcpaddr := server.tcpaddr
    server.RUnlock()

    log("Establishing new connection to ", addr, "...")
    conn, err := net.DialTCP("tcp", nil, tcpaddr)
    if err != nil {
        log("Connection to ", addr, " failed: ", err.String())
        return nil, err
    }
    log("Connection to ", addr, " established.")

    stats.conn(+1, server.Master)
    return newSocket(server, conn), nil
}

func (server *mongoServer) Close() {
    server.Lock()
    server.closed = true
    for i, s := range server.sockets {
        s.Close()
        server.sockets[i] = nil
    }
    server.sockets = server.sockets[0:0]
    server.Unlock()
}

func (server *mongoServer) RecycleSocket(socket *mongoSocket) {
    server.Lock()
    if server.closed {
        socket.Close()
    } else {
        server.sockets = append(server.sockets, socket)
    }
    server.Unlock()
}

func (server *mongoServer) Merge(other *mongoServer) {
    server.Lock()
    server.Master = other.Master
    // Sockets of other are ignored for the moment. Merging them
    // would mean a large number of sockets being cached on longer
    // recovering situations.
    other.Close()
    server.Unlock()
}

func (server *mongoServer) SetMaster(isMaster bool) {
    server.Lock()
    server.Master = isMaster
    server.Unlock()
}

type mongoServerSlice []*mongoServer

func (s mongoServerSlice) Len() int {
    return len(s)
}

func (s mongoServerSlice) Less(i, j int) bool {
    return s[i].ResolvedAddr < s[j].ResolvedAddr
}

func (s mongoServerSlice) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}

func (s mongoServerSlice) Sort() {
    sort.Sort(s)
}

func (s mongoServerSlice) Search(other *mongoServer) (i int, ok bool) {
    resolvedAddr := other.ResolvedAddr
    n := len(s)
    i = sort.Search(n, func(i int) bool {
        return s[i].ResolvedAddr >= resolvedAddr
    })
    return i, i != n && s[i].ResolvedAddr == resolvedAddr
}


type mongoServers struct {
    slice mongoServerSlice
}

func (servers *mongoServers) Search(other *mongoServer) (server *mongoServer) {
    if i, ok := servers.slice.Search(other); ok {
        return servers.slice[i]
    }
    return nil
}

func (servers *mongoServers) Add(server *mongoServer) {
    servers.slice = append(servers.slice, server)
    servers.slice.Sort()
}

func (servers *mongoServers) Remove(other *mongoServer) bool {
    if i, found := servers.slice.Search(other); found {
        n := len(servers.slice)
        copy(servers.slice[i:], servers.slice[i+1:n])
        servers.slice[n-1] = nil // Help GC.
        servers.slice = servers.slice[:n-1]
        return true
    }
    return false
}

func (servers *mongoServers) Slice() []*mongoServer {
    return ([]*mongoServer)(servers.slice)
}

func (servers *mongoServers) Get(i int) *mongoServer {
    return servers.slice[i]
}

func (servers *mongoServers) Len() int {
    return len(servers.slice)
}

func (servers *mongoServers) Empty() bool {
    return len(servers.slice) == 0
}
