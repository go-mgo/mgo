package mgo_test

import (
	"crypto/tls"
	"gopkg.in/mgo.v2"
	"io/ioutil"
	"net"

	. "gopkg.in/check.v1"
)

func (s *S) TestAuthx509Cred(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()
	binfo, err := session.BuildInfo()
	c.Assert(err, IsNil)
	if binfo.OpenSSLVersion == "" {
		c.Skip("server does not support SSL")
	}

	clientCertPEM, err := ioutil.ReadFile("testdb/client.pem")
	c.Assert(err, IsNil)

	clientCert, err := tls.X509KeyPair(clientCertPEM, clientCertPEM)
	c.Assert(err, IsNil)

	tlsConfig := &tls.Config{
		// Isolating tests to client certs, don't care about server validation.
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{clientCert},
	}

	var host = "localhost:40003"
	c.Logf("Connecting to %s...", host)
	session, err = mgo.DialWithInfo(&mgo.DialInfo{
		Addrs: []string{host},
		DialServer: func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), tlsConfig)
		},
	})
	c.Assert(err, IsNil)
	defer session.Close()

	err = session.Login(&mgo.Credential{Username: "root", Password: "rapadura"})
	c.Assert(err, IsNil)

	// This needs to be kept in sync with client.pem
	x509Subject := "CN=localhost,OU=Client,O=MGO,L=MGO,ST=MGO,C=GO"

	externalDB := session.DB("$external")
	var x509User mgo.User = mgo.User{
		Username:     x509Subject,
		OtherDBRoles: map[string][]mgo.Role{"admin": []mgo.Role{mgo.RoleRoot}},
	}
	err = externalDB.UpsertUser(&x509User)
	c.Assert(err, IsNil)

	session.LogoutAll()

	c.Logf("Connected! Ensuring authentication is required...")
	names, err := session.DatabaseNames()
	c.Assert(err, ErrorMatches, "not authorized .*")

	cred := &mgo.Credential{
		Username:  x509Subject,
		Mechanism: "MONGODB-X509",
		Source:    "$external",
	}

	c.Logf("Authenticating...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	names, err = session.DatabaseNames()
	c.Assert(err, IsNil)
	c.Assert(len(names) > 0, Equals, true)
}
