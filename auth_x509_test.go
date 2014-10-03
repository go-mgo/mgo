package mgo_test

import (
	"crypto/tls"
	"flag"
	"gopkg.in/mgo.v2"
	"io/ioutil"
	"net"

	. "gopkg.in/check.v1"
)

/* to run this test:
   you need to have an ssl enabled mongod
   you need to setup the mongod's with "testdb/setup.sh start ssl" instead of using make
   you need to run to "go test" with the -x509 flag
*/

var (
	x509Flag    = flag.Bool("x509", false, "Test x509 authentication (depends on having an ssl enabled mongd)")
	x509Subject = "CN=localhost,OU=Client,O=MGO,L=MGO,ST=MGO,C=GO" // this needs to be kept in sync with client.pem
	x509AuthUrl = "localhost:40301"
)

func (s *S) TestAuthx509Cred(c *C) {
	if !*x509Flag {
		c.Skip("no -x509")
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

	c.Logf("Connecting to %s...", x509AuthUrl)
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs: []string{x509AuthUrl},
		DialServer: func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), tlsConfig)
		},
	})
	c.Assert(err, IsNil)
	defer session.Close()

	adminDB := session.DB("admin")
	var adminUser mgo.User = mgo.User{Username: "Admin", Password: "AdminPassword", Roles: []mgo.Role{mgo.RoleRoot}}
	err = adminDB.UpsertUser(&adminUser)
	c.Assert(err, IsNil)

	err = session.Login(&mgo.Credential{Username: "Admin", Password: "AdminPassword"})
	c.Assert(err, IsNil)

	externalDB := session.DB("$external")
	var x509User mgo.User = mgo.User{Username: x509Subject, OtherDBRoles: map[string][]mgo.Role{"admin": []mgo.Role{mgo.RoleRoot}}}
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
