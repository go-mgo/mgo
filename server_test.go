package mgo

import (
	. "launchpad.net/gocheck"
        "net"
	"testing"
)


func TestAll(t *testing.T) {
	TestingT(t)
}

type ServerSuite struct {
}

var _ = Suite(&ServerSuite{})


func (s *ServerSuite) TestCloseDuringConnect(c *C) {
    var server *mongoServer
    closeDial := func (addr net.Addr) (net.Conn, error) {
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
        conn, err := localhostServer.Accept()
        c.Assert(err, IsNil)
        if err != nil {
            return
        }
        conn.Close()
    }()
    tcpaddr := localhostServer.Addr().(*net.TCPAddr)
    server = newServer(tcpaddr.String(), tcpaddr, make(chan bool), closeDial)
    conn, _, err := server.AcquireSocket(0)
    c.Assert(err, Equals, errServerClosed)
    c.Assert(conn, IsNil)
}
