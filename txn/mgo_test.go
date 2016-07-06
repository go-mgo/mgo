package txn_test

import (
	"bytes"
	"fmt"
	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2-unstable"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"
)

// ----------------------------------------------------------------------------
// The mgo test suite

type MgoSuite struct {
	output  bytes.Buffer
	server  *exec.Cmd
	session *mgo.Session
}

const mgoip = "127.0.0.1"

var mgoport int
var mgoaddr string

func init() {
	mgoport = FindTCPPort()
	mgoaddr = fmt.Sprintf("%v:%d", mgoip, mgoport)
}

// FindTCPPort finds an unused TCP port and returns it.
// Use of this function has an inherent race condition - another
// process may claim the port before we try to use it.
// We hope that the probability is small enough during
// testing to be negligible.
func FindTCPPort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (s *MgoSuite) StartServer(c *C) error {
	//mgo.SetDebug(true)
	c.Logf("Starting server on %v", mgoaddr)
	mgo.SetStats(true)
	dbdir := c.MkDir()
	args := []string{
		"--dbpath", dbdir,
		"--bind_ip", mgoip,
		"--port", strconv.Itoa(mgoport),
		"--nssize", "1",
		"--noprealloc",
		"--smallfiles",
		"--nojournal",
		"-vvvvv",
	}
	s.server = exec.Command("mongod", args...)
	s.server.Stdout = &s.output
	s.server.Stderr = &s.output
	err := s.server.Start()
	if err != nil {
		return err
	}

	// Wait until the server's ready for connections.
	var session *mgo.Session
	for attempt := 1; attempt < 11; attempt++ {
		session, err = mgo.Dial(mgoaddr)
		if err != nil {
			c.Logf("Retrying Dial - attempt %v failed with %v", attempt, err)
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	if err != nil {
		sigErr := s.server.Process.Signal(os.Interrupt)
		waitErr := s.server.Wait()
		c.Logf("server output: %v %v\n%v", sigErr, waitErr, string(s.output.Bytes()))
		return err
	}
	session.Close()
	return nil
}

func (s *MgoSuite) SetUpSuite(c *C) {
	err := s.StartServer(c)
	if err != nil {
		panic(err)
	}
}

func (s *MgoSuite) TearDownSuite(c *C) {
	s.server.Process.Kill()
	s.server.Process.Wait()
}

func (s *MgoSuite) SetUpTest(c *C) {
	err := DropAll(mgoaddr)
	if err != nil {
		panic(err)
	}
	mgo.SetLogger(c)
	mgo.ResetStats()

	s.session, err = mgo.Dial(mgoaddr)
	c.Assert(err, IsNil)
}

func (s *MgoSuite) TearDownTest(c *C) {
	if s.session != nil {
		s.session.Close()
	}
	for i := 0; ; i++ {
		stats := mgo.GetStats()
		if stats.SocketsInUse == 0 && stats.SocketsAlive == 0 {
			break
		}
		if i == 20 {
			c.Fatal("Test left sockets in a dirty state")
		}
		c.Logf("Waiting for sockets to die: %d in use, %d alive", stats.SocketsInUse, stats.SocketsAlive)
		time.Sleep(500 * time.Millisecond)
	}
}

func DropAll(mongourl string) (err error) {
	session, err := mgo.Dial(mongourl)
	if err != nil {
		return err
	}
	defer session.Close()

	names, err := session.DatabaseNames()
	if err != nil {
		return err
	}
	for _, name := range names {
		switch name {
		case "admin", "local", "config":
		default:
			err = session.DB(name).DropDatabase()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
