package mongogo

import (
    "strings"
    "gobson"
    "sync"
    "os"
)



// ---------------------------------------------------------------------------
// Entry point function to the cluster/session/server/socket hierarchy.

// Establish a session to the cluster identified by the given seed server(s).
// The session will enable communication with all of the servers in the cluster,
// so the seed servers are used only to find out about the cluster topology.
func Mongo(servers string) (session *Session, err os.Error) {
    userSeeds := strings.Split(servers, ",", -1)
    cluster := &mongoCluster{userSeeds:userSeeds}
    go cluster.syncServers()
    session = newSession(StrongConsistency, cluster, nil)
    return session, nil
}


// ---------------------------------------------------------------------------
// Mongo cluster encapsulation.
//
// A cluster enables the communication with one or more servers participating
// in a mongo cluster.  This works with individual servers, a replica set,
// a replica pair, one or multiple mongos routers, etc.

type mongoCluster struct {
    sync.RWMutex
    userSeeds, dynaSeeds []string
    servers mongoServers
    masters mongoServers
    slaves mongoServers
    masterSynced cond
}

func (cluster *mongoCluster) removeServer(server *mongoServer) {
    cluster.Lock()
    removed := cluster.servers.Remove(server) ||
               cluster.masters.Remove(server) ||
               cluster.slaves.Remove(server)
    if removed {
        log("Removing server ", server.Addr, " from cluster.")
    }
    cluster.Unlock()
}

func (cluster *mongoCluster) syncServer(server *mongoServer) (
        hosts []string, err os.Error) {

    addr := server.Addr

    log("[sync] Processing ", addr, "...")

    defer func() {
        if err != nil {
            // XXX TESTME
            cluster.removeServer(server)
        }
    }()

    socket, err := server.AcquireSocket()
    if err != nil {
        log("[sync] Failed to get socket to ", addr, ": ", err.String())
        return
    }

    session := newSession(StrongConsistency, cluster, socket)

    // XXX Use Run() here.
    cmd := session.DB("admin").C("$cmd")
    value, err := cmd.Find(gobson.M{"ismaster": 1}).One()
    if err != nil {
        log("[sync] Command 'ismaster' to ", addr, " failed: ", err.String())
        return
    }

    debugf("[sync] Result of 'ismaster' from %s: %#v", addr, value)

    if master, _ := value["ismaster"].(bool); master {
        // XXX Lock server here?
        // Must fix statistics, most importantly for tests.
        stats.trackConn(-1, server.Master)
        server.Master = true
        stats.trackConn(+1, server.Master)
        log("[sync] ", addr, " is a master.")
    } else if slave, _ := value["secondary"].(bool); slave {
        log("[sync] ", addr, " is a slave.")
    } else {
        log("[sync] ", addr, " is neither a master nor a slave.")
    }

    // XXX That's hell.  Use structs here instead.
    hosts = make([]string, 0, 5)
    if primary, ok := value["primary"].(string); ok {
        hosts = append(hosts, primary)
    }
    if hostsv, ok := value["hosts"]; ok {
        if slice, ok := hostsv.([]interface{}); ok {
            for _, host := range slice {
                if str, ok := host.(string); ok {
                    hosts = append(hosts, str)
                }
            }
        }
    }
    if hostsv, ok := value["passives"]; ok {
        if slice, ok := hostsv.([]interface{}); ok {
            for _, host := range slice {
                if str, ok := host.(string); ok {
                    hosts = append(hosts, str)
                }
            }
        }
    }

    session.Reset() // Recycle the socket.

    cluster.mergeServer(server)

    debugf("[sync] %s knows about the following peers: %#v", addr, hosts)
    return hosts, nil
}

func (cluster *mongoCluster) mergeServer(server *mongoServer) {
    cluster.Lock()
    previous := cluster.servers.Search(server)
    if previous == nil {
        cluster.servers.Add(server)
        if server.Master {
            log("[sync] Adding ", server.Addr, " to cluster as a master.")
            cluster.masters.Add(server)
        } else {
            log("[sync] Adding ", server.Addr, " to cluster as a slave.")
            cluster.slaves.Add(server)
        }
    } else {
        if server.Master != previous.Master {
            if previous.Master {
                log("[sync] Server ", server.Addr, " is now a slave.")
                cluster.masters.Remove(previous)
                cluster.slaves.Add(previous)
            } else {
                log("[sync] Server ", server.Addr, " is now a master.")
                cluster.slaves.Remove(previous)
                cluster.masters.Add(previous)
            }
        }
        previous.Merge(server)
    }
    if server.Master {
        debug("[sync] Broadcasting availability of master.")
        cluster.masterSynced.Broadcast()
    }
    cluster.Unlock()
}

func (cluster *mongoCluster) getKnownAddrs() []string {
    cluster.RLock()
    max := len(cluster.userSeeds)+len(cluster.dynaSeeds)+cluster.servers.Len()
    seen := make(map[string]bool, max)
    known := make([]string, 0, max)

    add := func(addr string) {
        if _, found := seen[addr]; !found {
            seen[addr] = true
            known = append(known, addr)
        }
    }

    for _, addr := range cluster.userSeeds { add(addr) }
    for _, addr := range cluster.dynaSeeds { add(addr) }
    for _, serv := range cluster.servers.Slice() { add(serv.Addr) }
    cluster.RUnlock()

    return known
}


// Synchronize all servers in the cluster.  This will contact all servers in
// parallel, ask them about known peers and their own role within the cluster,
// and then attempt to do the same with all the peers retrieved.  This function
// will only return once the full synchronization is done.
func (cluster *mongoCluster) syncServers() {
    log("[sync] Starting full topology synchronization...")

    known := cluster.getKnownAddrs()

    // Note that the logic below is lock free.  The locks below are
    // just to avoid race conditions internally and to wait for the
    // procedure to finish.

    var started, finished int
    var done sync.Mutex
    var m sync.Mutex

    done.Lock()
    seen := make(map[string]bool)

    var spawnSync func(addr string)
    spawnSync = func(addr string) {
        m.Lock()
        started++
        m.Unlock()

        go func() {
            defer func() {
                m.Lock()
                finished++
                if started == finished && finished >= len(known) {
                    done.Unlock()
                }
                m.Unlock()
            }()

            server, err := newServer(addr)
            if err != nil {
                log("[sync] Failed to start sync of ", addr, ": ", err.String())
                return
            }

            if _, found := seen[server.ResolvedAddr]; found {
                return
            }
            seen[server.ResolvedAddr] = true

            hosts, err := cluster.syncServer(server)
            if err == nil {
                for _, addr := range hosts {
                    spawnSync(addr)
                }
            }
        }()
    }

    for _, addr := range known {
        spawnSync(addr)
    }

    done.Lock()
    log("[sync] Synchronization completed: ", cluster.masters.Len(),
        " master(s) and, ", cluster.slaves.Len(), " slave(s) alive.")

    // Update dynamic seeds, but only if we have any good servers. Otherwise,
    // leave them alone for better chances of a successful sync in the future.
    cluster.Lock()
    if !cluster.servers.Empty() {
        dynaSeeds := make([]string, cluster.servers.Len())
        for i, server := range cluster.servers.Slice() {
            dynaSeeds[i] = server.Addr
        }
        cluster.dynaSeeds = dynaSeeds
        debugf("New dynamic seeds: %#v\n", dynaSeeds)
    }
    cluster.Unlock()
}

// Return a socket to a server in the cluster.  If write is true, it will return
// a socket to a server which will accept writes.  If it is false, the socket
// will be to an arbitrary server, preferably a slave.
func (cluster *mongoCluster) AcquireSocket(write bool) (s *mongoSocket, err os.Error) {
    cluster.masterSynced.Wait(func() bool {
        cluster.RLock()
        debugf("Cluster has %d known masters.", cluster.masters.Len())
        if !cluster.masters.Empty() {
            return true
        }
        cluster.RUnlock()
        log("Waiting for masters to synchronize.")
        return false
    })

    var server *mongoServer
    if write || cluster.slaves.Empty() {
        server = cluster.masters.Get(0) // XXX Pick random.
    } else {
        server = cluster.slaves.Get(0) // XXX Pick random.
    }
    cluster.RUnlock()

    s, err = server.AcquireSocket()
    if err != nil {
        // XXX Switch server on connection errors.
        return nil, err
    }
    return s, err
}
