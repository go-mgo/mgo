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
			c.Logf("got connection: %v", conn)
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	tcpaddr := localhostServer.Addr().(*net.TCPAddr)
	server = newServer(tcpaddr.String(), tcpaddr, make(chan bool), closeDial)
	c.Assert(server, NotNil)
	conn, _, err := server.AcquireSocket(0)
	c.Check(err, Equals, errServerClosed)
	c.Assert(conn, IsNil)
}
