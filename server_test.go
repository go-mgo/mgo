package mgo

import (
	. "launchpad.net/gocheck"
	"net"
)

type ServerSuite struct {
}

var _ = Suite(&ServerSuite{})

func (s *ServerSuite) TestCloseDuringConnect(c *C) {
	var server *mongoServer
	closeDial := func(addr net.Addr) (net.Conn, error) {
		// The first call to this will be during newServer, and
		// server will be nil. Once it returns, the second call
		// will be with a valid server, and we will Close it.
		if server != nil {
			server.Close()
		}
		return net.DialTCP("tcp", nil, addr.(*net.TCPAddr))
	}
	localhostServer, err := net.Listen("tcp", "localhost:0")
	c.Assert(err, IsNil)
	defer localhostServer.Close()
	go func() {
		// Accept a connection but don't do anything with it
		for {
			conn, err := localhostServer.Accept()
                        logf("got connection: %v", conn)
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	tcpaddr := localhostServer.Addr().(*net.TCPAddr)
	server = newServer(tcpaddr.String(), tcpaddr, make(chan bool), closeDial)
	c.Assert(server, NotNil)
	// It is possible for newServer to ping the remote host, and the
	// request returns immediately (because writing to the socket fails).
	// In which case pinger(loop=false) puts the closed socket back into
	// unused. We have to wait until readLoop gets a EOF and removes the
	// socket from the queue.
	for {
		server.RLock()
		count := len(server.unusedSockets)
		server.RUnlock()
		if count == 0 {
			break
		}
                logf("server had %d unused sockets waiting for 0", count)
	}
	conn, _, err := server.AcquireSocket(0)
	c.Check(err, Equals, errServerClosed)
	c.Assert(conn, IsNil)
}
