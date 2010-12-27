package mongogo

import (
    "sync"
)


var stats *Stats
var statsMutex sync.Mutex

func SetCollectStats(enabled bool) {
    statsMutex.Lock()
    if enabled {
        if stats == nil {
            stats = &Stats{}
        }
    } else {
        stats = nil
    }
    statsMutex.Unlock()
}

func GetStats() (snapshot Stats) {
    statsMutex.Lock()
    snapshot = *stats
    statsMutex.Unlock()
    return
}

func ResetStats() {
    statsMutex.Lock()
    stats = &Stats{}
    statsMutex.Unlock()
    return
}

type Stats struct {
    MasterConns int
    SlaveConns int
    BytesWritten int
    BytesRead int
}

func (stats *Stats) trackConn(delta int, master bool) {
    if stats != nil {
        statsMutex.Lock()
        if master {
            stats.MasterConns += delta
        } else {
            stats.SlaveConns += delta
        }
        statsMutex.Unlock()
    }
}
