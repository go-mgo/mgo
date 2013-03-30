// mgo - MongoDB driver for Go
// 
// Copyright (c) 2010-2012 - Gustavo Niemeyer <gustavo@niemeyer.net>
// 
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met: 
// 
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer. 
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution. 
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package mgo_test

import (
	"errors"
	"flag"
	"fmt"
	. "launchpad.net/gocheck"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net"
	"os/exec"

	"testing"
	"time"
)

var fast = flag.Bool("fast", false, "Skip slow tests")

type M bson.M

type cLogger C

func (c *cLogger) Output(calldepth int, s string) error {
	ns := time.Now().UnixNano()
	t := float64(ns%100e9) / 1e9
	((*C)(c)).Logf("[LOG] %.05f %s", t, s)
	return nil
}

func TestAll(t *testing.T) {
	TestingT(t)
}

type S struct {
	session *mgo.Session
	stopped bool
	build mgo.BuildInfo
}

func (s *S) versionAtLeast(v ...int) bool {
	for i := range v {
		if i == len(s.build.VersionArray) {
			return false
		}
		if s.build.VersionArray[i] < v[i] {
			return false
		}
	}
	return true
}

var _ = Suite(&S{})

func (s *S) SetUpSuite(c *C) {
	mgo.SetDebug(true)
	mgo.SetStats(true)
	s.StartAll()

	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	s.build, err = session.BuildInfo()
	c.Check(err, IsNil)
	session.Close()
}

func (s *S) SetUpTest(c *C) {
	err := run("mongo --nodb testdb/dropall.js")
	if err != nil {
		panic(err.Error())
	}
	mgo.SetLogger((*cLogger)(c))
	mgo.ResetStats()
}

func (s *S) TearDownTest(c *C) {
	if s.stopped {
		s.StartAll()
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
		time.Sleep(5e8)
	}
}

func (s *S) Stop(host string) {
	err := run("cd _testdb && supervisorctl stop " + supvName(host))
	if err != nil {
		panic(err.Error())
	}
	s.stopped = true
}

func (s *S) StartAll() {
	// Restart any stopped nodes.
	run("cd _testdb && supervisorctl start all")
	err := run("cd testdb && mongo --nodb wait.js")
	if err != nil {
		panic(err.Error())
	}
	s.stopped = false
}

func run(command string) error {
	output, err := exec.Command("/bin/sh", "-c", command).CombinedOutput()
	if err != nil {
		msg := fmt.Sprintf("Failed to execute: %s: %s\n%s", command, err.Error(), string(output))
		return errors.New(msg)
	}
	return nil
}

var supvNames = map[string]string{
	"40001": "db1",
	"40002": "db2",
	"40011": "rs1a",
	"40012": "rs1b",
	"40013": "rs1c",
	"40021": "rs2a",
	"40022": "rs2b",
	"40023": "rs2c",
	"40031": "rs3a",
	"40032": "rs3b",
	"40033": "rs3c",
	"40041": "rs4a",
	"40101": "cfg1",
	"40102": "cfg2",
	"40103": "cfg3",
	"40201": "s1",
	"40202": "s2",
	"40203": "s3",
}

// supvName returns the supervisord name for the given host address.
func supvName(host string) string {
	host, port, err := net.SplitHostPort(host)
	if err != nil {
		panic(err)
	}
	name, ok := supvNames[port]
	if !ok {
		panic("Unknown host: " + host)
	}
	return name
}
