package mgo

import (
	"net"
	"time"
)

func HackSocketsPerServer(newLimit int) (restore func()) {
	oldLimit := newLimit
	restore = func() {
		socketsPerServer = oldLimit
	}
	socketsPerServer = newLimit
	return
}

func HackPingDelay(newDelay time.Duration) (restore func()) {
	oldDelay := pingDelay
	restore = func() {
		pingDelay = oldDelay
	}
	pingDelay = newDelay
	return
}

func NewServer(addr string, tcpaddr *net.TCPAddr, sync chan bool, dial dialer) *mongoServer {
	return newServer(addr, tcpaddr, sync, dial)
}
