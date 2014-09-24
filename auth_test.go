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
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"sync"
	"time"

	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
)

func (s *S) TestAuthLoginDatabase(c *C) {
	// Test both with a normal database and with an authenticated shard.
	for _, addr := range []string{"localhost:40002", "localhost:40203"} {
		session, err := mgo.Dial(addr)
		c.Assert(err, IsNil)
		defer session.Close()

		coll := session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")

		admindb := session.DB("admin")

		err = admindb.Login("root", "wrong")
		c.Assert(err, ErrorMatches, "auth fail(s|ed)")

		err = admindb.Login("root", "rapadura")
		c.Assert(err, IsNil)

		err = coll.Insert(M{"n": 1})
		c.Assert(err, IsNil)
	}
}

func (s *S) TestAuthLoginSession(c *C) {
	// Test both with a normal database and with an authenticated shard.
	for _, addr := range []string{"localhost:40002", "localhost:40203"} {
		session, err := mgo.Dial(addr)
		c.Assert(err, IsNil)
		defer session.Close()

		coll := session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")

		cred := mgo.Credential{
			Username: "root",
			Password: "wrong",
		}
		err = session.Login(&cred)
		c.Assert(err, ErrorMatches, "auth fail(s|ed)")

		cred.Password = "rapadura"

		err = session.Login(&cred)
		c.Assert(err, IsNil)

		err = coll.Insert(M{"n": 1})
		c.Assert(err, IsNil)
	}
}

func (s *S) TestAuthLoginLogout(c *C) {
	// Test both with a normal database and with an authenticated shard.
	for _, addr := range []string{"localhost:40002", "localhost:40203"} {
		session, err := mgo.Dial(addr)
		c.Assert(err, IsNil)
		defer session.Close()

		admindb := session.DB("admin")
		err = admindb.Login("root", "rapadura")
		c.Assert(err, IsNil)

		admindb.Logout()

		coll := session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")

		// Must have dropped auth from the session too.
		session = session.Copy()
		defer session.Close()

		coll = session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")
	}
}

func (s *S) TestAuthLoginLogoutAll(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session.LogoutAll()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")

	// Must have dropped auth from the session too.
	session = session.Copy()
	defer session.Close()

	coll = session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized .*")
}

func (s *S) TestAuthUpsertUserErrors(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")

	err = mydb.UpsertUser(&mgo.User{})
	c.Assert(err, ErrorMatches, "user has no Username")

	err = mydb.UpsertUser(&mgo.User{Username: "user", Password: "pass", UserSource: "source"})
	c.Assert(err, ErrorMatches, "user has both Password/PasswordHash and UserSource set")

	err = mydb.UpsertUser(&mgo.User{Username: "user", Password: "pass", OtherDBRoles: map[string][]mgo.Role{"db": nil}})
	c.Assert(err, ErrorMatches, "user with OtherDBRoles is only supported in admin database")
}

func (s *S) TestAuthUpsertUser(c *C) {
	if !s.versionAtLeast(2, 4) {
		c.Skip("UpsertUser only works on 2.4+")
	}
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")

	ruser := &mgo.User{
		Username: "myruser",
		Password: "mypass",
		Roles:    []mgo.Role{mgo.RoleRead},
	}
	rwuser := &mgo.User{
		Username: "myrwuser",
		Password: "mypass",
		Roles:    []mgo.Role{mgo.RoleReadWrite},
	}

	err = mydb.UpsertUser(ruser)
	c.Assert(err, IsNil)
	err = mydb.UpsertUser(rwuser)
	c.Assert(err, IsNil)

	err = mydb.Login("myruser", "mypass")
	c.Assert(err, IsNil)

	admindb.Logout()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	err = mydb.Login("myrwuser", "mypass")
	c.Assert(err, IsNil)

	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)

	myotherdb := session.DB("myotherdb")

	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	// Test UserSource.
	rwuserother := &mgo.User{
		Username:   "myrwuser",
		UserSource: "mydb",
		Roles:      []mgo.Role{mgo.RoleRead},
	}

	err = myotherdb.UpsertUser(rwuserother)
	if s.versionAtLeast(2, 6) {
		c.Assert(err, ErrorMatches, `MongoDB 2.6\+ does not support the UserSource setting`)
		return
	}
	c.Assert(err, IsNil)

	admindb.Logout()

	// Test indirection via UserSource: we can't write to it, because
	// the roles for myrwuser are different there.
	othercoll := myotherdb.C("myothercoll")
	err = othercoll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	// Reading works, though.
	err = othercoll.Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)

	// Can't login directly into the database using UserSource, though.
	err = myotherdb.Login("myrwuser", "mypass")
	c.Assert(err, ErrorMatches, "auth fail(s|ed)")
}

func (s *S) TestAuthUpsertUserOtherDBRoles(c *C) {
	if !s.versionAtLeast(2, 4) {
		c.Skip("UpsertUser only works on 2.4+")
	}
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	ruser := &mgo.User{
		Username:     "myruser",
		Password:     "mypass",
		OtherDBRoles: map[string][]mgo.Role{"mydb": []mgo.Role{mgo.RoleRead}},
	}

	err = admindb.UpsertUser(ruser)
	c.Assert(err, IsNil)
	defer admindb.RemoveUser("myruser")

	admindb.Logout()
	err = admindb.Login("myruser", "mypass")

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	err = coll.Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

func (s *S) TestAuthUpsertUserUpdates(c *C) {
	if !s.versionAtLeast(2, 4) {
		c.Skip("UpsertUser only works on 2.4+")
	}
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")

	// Insert a user that can read.
	user := &mgo.User{
		Username: "myruser",
		Password: "mypass",
		Roles:    []mgo.Role{mgo.RoleRead},
	}
	err = mydb.UpsertUser(user)
	c.Assert(err, IsNil)

	// Now update the user password.
	user = &mgo.User{
		Username: "myruser",
		Password: "mynewpass",
	}
	err = mydb.UpsertUser(user)
	c.Assert(err, IsNil)

	// Login with the new user.
	usession, err := mgo.Dial("myruser:mynewpass@localhost:40002/mydb")
	c.Assert(err, IsNil)
	defer usession.Close()

	// Can read, but not write.
	err = usession.DB("mydb").C("mycoll").Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
	err = usession.DB("mydb").C("mycoll").Insert(M{"ok": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	// Update the user role.
	user = &mgo.User{
		Username: "myruser",
		Roles:    []mgo.Role{mgo.RoleReadWrite},
	}
	err = mydb.UpsertUser(user)
	c.Assert(err, IsNil)

	// Dial again to ensure the password hasn't changed.
	usession, err = mgo.Dial("myruser:mynewpass@localhost:40002/mydb")
	c.Assert(err, IsNil)
	defer usession.Close()

	// Now it can write.
	err = usession.DB("mydb").C("mycoll").Insert(M{"ok": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthAddUser(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myruser", "mypass", true)
	c.Assert(err, IsNil)
	err = mydb.AddUser("mywuser", "mypass", false)
	c.Assert(err, IsNil)

	err = mydb.Login("myruser", "mypass")
	c.Assert(err, IsNil)

	admindb.Logout()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	err = mydb.Login("mywuser", "mypass")
	c.Assert(err, IsNil)

	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthAddUserReplaces(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "myoldpass", false)
	c.Assert(err, IsNil)
	err = mydb.AddUser("myuser", "mynewpass", true)
	c.Assert(err, IsNil)

	admindb.Logout()

	err = mydb.Login("myuser", "myoldpass")
	c.Assert(err, ErrorMatches, "auth fail(s|ed)")
	err = mydb.Login("myuser", "mynewpass")
	c.Assert(err, IsNil)

	// ReadOnly flag was changed too.
	err = mydb.C("mycoll").Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")
}

func (s *S) TestAuthRemoveUser(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "mypass", true)
	c.Assert(err, IsNil)
	err = mydb.RemoveUser("myuser")
	c.Assert(err, IsNil)
	err = mydb.RemoveUser("myuser")
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = mydb.Login("myuser", "mypass")
	c.Assert(err, ErrorMatches, "auth fail(s|ed)")
}

func (s *S) TestAuthLoginTwiceDoesNothing(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	oldStats := mgo.GetStats()

	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)
}

func (s *S) TestAuthLoginLogoutLoginDoesNothing(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	oldStats := mgo.GetStats()

	admindb.Logout()
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)
}

func (s *S) TestAuthLoginSwitchUser(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)

	err = admindb.Login("reader", "rapadura")
	c.Assert(err, IsNil)

	// Can't write.
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	// But can read.
	result := struct{ N int }{}
	err = coll.Find(nil).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 1)
}

func (s *S) TestAuthLoginChangePassword(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "myoldpass", false)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "myoldpass")
	c.Assert(err, IsNil)

	err = mydb.AddUser("myuser", "mynewpass", true)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mynewpass")
	c.Assert(err, IsNil)

	admindb.Logout()

	// The second login must be in effect, which means read-only.
	err = mydb.C("mycoll").Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")
}

func (s *S) TestAuthLoginCachingWithSessionRefresh(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session.Refresh()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingWithSessionCopy(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session = session.Copy()
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingWithSessionClone(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session = session.Clone()
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingWithNewSession(c *C) {
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	session = session.New()
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	err = coll.Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|need to login|not authorized for .*")
}

func (s *S) TestAuthLoginCachingAcrossPool(c *C) {
	// Logins are cached even when the conenction goes back
	// into the pool.

	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	// Add another user to test the logout case at the same time.
	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "mypass", false)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Logout root explicitly, to test both cases.
	admindb.Logout()

	// Give socket back to pool.
	session.Refresh()

	// Brand new session, should use socket from the pool.
	other := session.New()
	defer other.Close()

	oldStats := mgo.GetStats()

	err = other.DB("admin").Login("root", "rapadura")
	c.Assert(err, IsNil)
	err = other.DB("mydb").Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Both logins were cached, so no ops.
	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)

	// And they actually worked.
	err = other.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)

	other.DB("admin").Logout()

	err = other.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthLoginCachingAcrossPoolWithLogout(c *C) {
	// Now verify that logouts are properly flushed if they
	// are not revalidated after leaving the pool.

	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	// Add another user to test the logout case at the same time.
	mydb := session.DB("mydb")
	err = mydb.AddUser("myuser", "mypass", true)
	c.Assert(err, IsNil)

	err = mydb.Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Just some data to query later.
	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)

	// Give socket back to pool.
	session.Refresh()

	// Brand new session, should use socket from the pool.
	other := session.New()
	defer other.Close()

	oldStats := mgo.GetStats()

	err = other.DB("mydb").Login("myuser", "mypass")
	c.Assert(err, IsNil)

	// Login was cached, so no ops.
	newStats := mgo.GetStats()
	c.Assert(newStats.SentOps, Equals, oldStats.SentOps)

	// Can't write, since root has been implicitly logged out
	// when the collection went into the pool, and not revalidated.
	err = other.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	// But can read due to the revalidated myuser login.
	result := struct{ N int }{}
	err = other.DB("mydb").C("mycoll").Find(nil).One(&result)
	c.Assert(err, IsNil)
	c.Assert(result.N, Equals, 1)
}

func (s *S) TestAuthEventual(c *C) {
	// Eventual sessions don't keep sockets around, so they are
	// an interesting test case.
	session, err := mgo.Dial("localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	admindb := session.DB("admin")
	err = admindb.Login("root", "rapadura")
	c.Assert(err, IsNil)

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(20)

	for i := 0; i != 10; i++ {
		go func() {
			defer wg.Done()
			var result struct{ N int }
			err := session.DB("mydb").C("mycoll").Find(nil).One(&result)
			c.Assert(err, IsNil)
			c.Assert(result.N, Equals, 1)
		}()
	}

	for i := 0; i != 10; i++ {
		go func() {
			defer wg.Done()
			err := session.DB("mydb").C("mycoll").Insert(M{"n": 1})
			c.Assert(err, IsNil)
		}()
	}

	wg.Wait()
}

func (s *S) TestAuthURL(c *C) {
	session, err := mgo.Dial("mongodb://root:rapadura@localhost:40002/")
	c.Assert(err, IsNil)
	defer session.Close()

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthURLWrongCredentials(c *C) {
	session, err := mgo.Dial("mongodb://root:wrong@localhost:40002/")
	if session != nil {
		session.Close()
	}
	c.Assert(err, ErrorMatches, "auth fail(s|ed)")
	c.Assert(session, IsNil)
}

func (s *S) TestAuthURLWithNewSession(c *C) {
	// When authentication is in the URL, the new session will
	// actually carry it on as well, even if logged out explicitly.
	session, err := mgo.Dial("mongodb://root:rapadura@localhost:40002/")
	c.Assert(err, IsNil)
	defer session.Close()

	session.DB("admin").Logout()

	// Do it twice to ensure it passes the needed data on.
	session = session.New()
	defer session.Close()
	session = session.New()
	defer session.Close()

	err = session.DB("mydb").C("mycoll").Insert(M{"n": 1})
	c.Assert(err, IsNil)
}

func (s *S) TestAuthURLWithDatabase(c *C) {
	session, err := mgo.Dial("mongodb://root:rapadura@localhost:40002")
	c.Assert(err, IsNil)
	defer session.Close()

	mydb := session.DB("mydb")
	err = mydb.AddUser("myruser", "mypass", true)
	c.Assert(err, IsNil)

	// Test once with database, and once with source.
	for i := 0; i < 2; i++ {
		var url string
		if i == 0 {
			url = "mongodb://myruser:mypass@localhost:40002/mydb"
		} else {
			url = "mongodb://myruser:mypass@localhost:40002/admin?authSource=mydb"
		}
		usession, err := mgo.Dial(url)
		c.Assert(err, IsNil)
		defer usession.Close()

		ucoll := usession.DB("mydb").C("mycoll")
		err = ucoll.FindId(0).One(nil)
		c.Assert(err, Equals, mgo.ErrNotFound)
		err = ucoll.Insert(M{"n": 1})
		c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")
	}
}

func (s *S) TestDefaultDatabase(c *C) {
	tests := []struct{ url, db string }{
		{"mongodb://root:rapadura@localhost:40002", "test"},
		{"mongodb://root:rapadura@localhost:40002/admin", "admin"},
		{"mongodb://localhost:40001", "test"},
		{"mongodb://localhost:40001/", "test"},
		{"mongodb://localhost:40001/mydb", "mydb"},
	}

	for _, test := range tests {
		session, err := mgo.Dial(test.url)
		c.Assert(err, IsNil)
		defer session.Close()

		c.Logf("test: %#v", test)
		c.Assert(session.DB("").Name, Equals, test.db)

		scopy := session.Copy()
		c.Check(scopy.DB("").Name, Equals, test.db)
		scopy.Close()
	}
}

func (s *S) TestAuthDirect(c *C) {
	// Direct connections must work to the master and slaves.
	for _, port := range []string{"40031", "40032", "40033"} {
		url := fmt.Sprintf("mongodb://root:rapadura@localhost:%s/?connect=direct", port)
		session, err := mgo.Dial(url)
		c.Assert(err, IsNil)
		defer session.Close()

		session.SetMode(mgo.Monotonic, true)

		var result struct{}
		err = session.DB("mydb").C("mycoll").Find(nil).One(&result)
		c.Assert(err, Equals, mgo.ErrNotFound)
	}
}

func (s *S) TestAuthDirectWithLogin(c *C) {
	// Direct connections must work to the master and slaves.
	for _, port := range []string{"40031", "40032", "40033"} {
		url := fmt.Sprintf("mongodb://localhost:%s/?connect=direct", port)
		session, err := mgo.Dial(url)
		c.Assert(err, IsNil)
		defer session.Close()

		session.SetMode(mgo.Monotonic, true)
		session.SetSyncTimeout(3 * time.Second)

		err = session.DB("admin").Login("root", "rapadura")
		c.Assert(err, IsNil)

		var result struct{}
		err = session.DB("mydb").C("mycoll").Find(nil).One(&result)
		c.Assert(err, Equals, mgo.ErrNotFound)
	}
}

// TODO SCRAM-SHA-1 will become the default, and this flag will go away.
var scramFlag = flag.String("scram", "", "Host to test SCRAM-SHA-1 authentication against (depends on custom environment)")

func (s *S) TestAuthScramSha1Cred(c *C) {
	if *scramFlag == "" {
		c.Skip("no -plain")
	}
	cred := &mgo.Credential{
		Username:  "root",
		Password:  "rapadura",
		Mechanism: "SCRAM-SHA-1",
		Source:    "admin",
	}
	c.Logf("Connecting to %s...", *scramFlag)
	session, err := mgo.Dial(*scramFlag)
	c.Assert(err, IsNil)
	defer session.Close()

	mycoll := session.DB("admin").C("mycoll")

	c.Logf("Connected! Testing the need for authentication...")
	err = mycoll.Find(nil).One(nil)
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	c.Logf("Authenticating...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	c.Logf("Connected! Testing the need for authentication...")
	err = mycoll.Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

var (
	plainFlag = flag.String("plain", "", "Host to test PLAIN authentication against (depends on custom environment)")
	plainUser = "einstein"
	plainPass = "password"
)

func (s *S) TestAuthPlainCred(c *C) {
	if *plainFlag == "" {
		c.Skip("no -plain")
	}
	cred := &mgo.Credential{
		Username:  plainUser,
		Password:  plainPass,
		Source:    "$external",
		Mechanism: "PLAIN",
	}
	c.Logf("Connecting to %s...", *plainFlag)
	session, err := mgo.Dial(*plainFlag)
	c.Assert(err, IsNil)
	defer session.Close()

	records := session.DB("records").C("records")

	c.Logf("Connected! Testing the need for authentication...")
	err = records.Find(nil).One(nil)
	c.Assert(err, ErrorMatches, "unauthorized|not authorized .*")

	c.Logf("Authenticating...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	c.Logf("Connected! Testing the need for authentication...")
	err = records.Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

func (s *S) TestAuthPlainURL(c *C) {
	if *plainFlag == "" {
		c.Skip("no -plain")
	}
	c.Logf("Connecting to %s...", *plainFlag)
	session, err := mgo.Dial(fmt.Sprintf("%s:%s@%s?authMechanism=PLAIN", url.QueryEscape(plainUser), url.QueryEscape(plainPass), *plainFlag))
	c.Assert(err, IsNil)
	defer session.Close()

	c.Logf("Connected! Testing the need for authentication...")
	err = session.DB("records").C("records").Find(nil).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)
}

var (
	kerberosFlag = flag.Bool("kerberos", false, "Test Kerberos authentication (depends on custom environment)")
	kerberosHost = "ldaptest.10gen.cc"
	kerberosUser = "drivers@LDAPTEST.10GEN.CC"

	winKerberosPasswordEnv = "MGO_KERBEROS_PASSWORD"
)


// Kerberos has its own suite because it talks to a remote server
// that is prepared to authenticate against a kerberos deployment.
type KerberosSuite struct{}

var _ = Suite(&KerberosSuite{})

func (kerberosSuite *KerberosSuite) SetUpSuite(c *C) {
	mgo.SetDebug(true)
	mgo.SetStats(true)
}

func (kerberosSuite *KerberosSuite) TearDownSuite(c *C) {
	mgo.SetDebug(false)
	mgo.SetStats(false)
}

func (kerberosSuite *KerberosSuite) SetUpTest(c *C) {
	mgo.SetLogger((*cLogger)(c))
	mgo.ResetStats()
}

func (kerberosSuite *KerberosSuite) TearDownTest(c *C) {
	mgo.SetLogger(nil)
}

func (kerberosSuite *KerberosSuite) TestAuthKerberosCred(c *C) {
	if !*kerberosFlag {
		c.Skip("no -kerberos")
	}
	cred := &mgo.Credential{
		Username:  kerberosUser,
		Mechanism: "GSSAPI",
	}
	windowsAppendPasswordToCredential(cred)
	c.Logf("Connecting to %s...", kerberosHost)
	session, err := mgo.Dial(kerberosHost)
	c.Assert(err, IsNil)
	defer session.Close()

	c.Logf("Connected! Testing the need for authentication...")
	n, err := session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, ErrorMatches, ".*authorized.*")

	c.Logf("Authenticating...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	n, err = session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

func (kerberosSuite *KerberosSuite) TestAuthKerberosURL(c *C) {
	if !*kerberosFlag {
		c.Skip("no -kerberos")
	}
	c.Logf("Connecting to %s...", kerberosHost)
	connectUri := url.QueryEscape(kerberosUser) + "@" + kerberosHost + "?authMechanism=GSSAPI"
	if runtime.GOOS == "windows" {
		connectUri = url.QueryEscape(kerberosUser) + ":" + url.QueryEscape(getWindowsKerberosPassword()) + "@" + kerberosHost + "?authMechanism=GSSAPI"
	}
	session, err := mgo.Dial(connectUri)
	c.Assert(err, IsNil)
	defer session.Close()
	n, err := session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

func (kerberosSuite *KerberosSuite) TestAuthKerberosServiceName(c *C) {
	if !*kerberosFlag {
		c.Skip("no -kerberos")
	}

	wrongServiceName := "wrong"
	rightServiceName := "mongodb"

	cred := &mgo.Credential{
		Username:  kerberosUser,
		Mechanism: "GSSAPI",
		Service:   wrongServiceName,
	}
	windowsAppendPasswordToCredential(cred)

	c.Logf("Connecting to %s...", kerberosHost)
	session, err := mgo.Dial(kerberosHost)
	c.Assert(err, IsNil)
	defer session.Close()

	c.Logf("Authenticating with incorrect service name...")
	err = session.Login(cred)
	c.Assert(err, ErrorMatches, ".*@LDAPTEST.10GEN.CC not found.*")

	cred.Service = rightServiceName
	c.Logf("Authenticating with correct service name...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	n, err := session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

func (kerberosSuite *KerberosSuite) TestAuthKerberosServiceHost(c *C) {
	if !*kerberosFlag {
		c.Skip("no -kerberos")
	}

	wrongServiceHost := "eggs.bacon.tk"
	rightServiceHost := kerberosHost

	cred := &mgo.Credential{
		Username:    kerberosUser,
		Mechanism:   "GSSAPI",
		ServiceHost: wrongServiceHost,
	}
	windowsAppendPasswordToCredential(cred)

	c.Logf("Connecting to %s...", kerberosHost)
	session, err := mgo.Dial(kerberosHost)
	c.Assert(err, IsNil)
	defer session.Close()

	c.Logf("Authenticating with incorrect service host...")
	err = session.Login(cred)
	c.Assert(err, ErrorMatches, ".*@LDAPTEST.10GEN.CC not found.*")

	cred.ServiceHost = rightServiceHost
	c.Logf("Authenticating with correct service host...")
	err = session.Login(cred)
	c.Assert(err, IsNil)
	c.Logf("Authenticated!")

	n, err := session.DB("kerberos").C("test").Find(M{}).Count()
	c.Assert(err, IsNil)
	c.Assert(n, Equals, 1)
}

// No kinit on SSPI-style Kerberos, so we need to provide a password. In order
// to avoid inlining password, require it to be set as an environment variable,
// for instance: `SET MGO_KERBEROS_PASSWORD=this_isnt_the_password`
func getWindowsKerberosPassword() string {
	pw := os.Getenv(winKerberosPasswordEnv)
	if pw == "" {
		panic(fmt.Sprintf("Need to set %v environment variable to run Kerberos tests on Windows", winKerberosPasswordEnv))
	}
	return pw
}

func windowsAppendPasswordToCredential(cred *mgo.Credential) {
	if runtime.GOOS == "windows" {
		cred.Password = getWindowsKerberosPassword()
	}
}

var (
	x509Flag    = flag.String("x509", "", "Test x509 authentication (depends on custom environment)")
	x509Subject = "CN=localhost,OU=Client,O=MGO,L=MGO,ST=MGO,C=GO"
	clientCert  = `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAwE2sl8YeTTSetwo9kykJ5mCZ/FtfPtn/0X4nOlTM2Qc/uWzA
sjSYoSV4UkuOiWjKQQH2EDeXaltshOo7F0oCY5ozVeQe+phe987iKTvLtf7NoXJD
KqNqR4Kb4ylbCrEky7+Xvw6yrrqw8qgWy+9VsrilR3q8LsETE9SBMtfp3BUaaNQp
peNm+iAhx3uZSv3mdzSLFSA/o61kAyG0scLExYDjo/7xyMNQoloLvNmx4Io160+y
lOz077/qqU620tmuDLRz1QdxK/bptmXTnsBCRxl+U8nzbwVZgWFENhXplbcN+SjN
LhdnvTiU2qFhgZmc7ZtCKdPIpx3W6pH9bx7kTwIDAQABAoIBAQCOQygyo8NY9FuS
J8ZDrvF+9+oS8fm1QorpDT2x/ngI+j7fSyAG9bgQRusLXpAVAWvWyb+iYa3nZbkT
X0DVys+XpcTifr+YPc7L3sYbIPxkKBsxm5kq2vfN7Uart7V9ZG1HOfblxdbUQpKT
AVzUA7vPWqATEC5VHEqjuerWlTqRr9YLZE/nkE7ICLISqdl4WDYfUYJwoXWfYkXQ
Lfl5Qh2leyri9S3urvDrhnURTQ1lM182IbTRA+9rUiFzsRW+9U4HPY7Ao2Itp8dr
GRP4rcq4TP+NcF0Ky64cNfKXCWmwqTBRFYAlTD6gwjN/s2BzvWD/2nlnc0DYAXrB
TgFCPk7xAoGBAOwuHICwwTxtzrdWjuRGU3RxL4eLEXedtL8yon/yPci3e+8eploX
1Fp0rEK2gIGDp/X8DiOtrKXih8XPusCwE/I3EvjHdI0RylLZXTPOp1Ei21dXRsiV
YxcF+d5s11q5tJtF+5ISUeIz2iSc9Z2LBnb8JDK1jcCRa5Q212q3ZWW5AoGBANBw
9CoMbxINLG1q0NvOOSwMKDk2OB+9JbQ5lwF4ijZl2I6qRoOCzQ3lBs0Qv/AeBjNR
SerDs2+eWnIBUbgSdiqcOKnXAI/Qbl1IkVFYV/2g9m6dgu1fNWNBv8NIYDHCLfDx
W3fpO5JMf+iE5XC4XqCfSBIME2yxPSGQjal6tB5HAoGAddYDzolhv/6hVoPPQ0F7
PeuC5UOTcXSzy3k97kw0W0KAiStnoCengYIYuChKMVQ4ptgdTdvG+fTt/NnJuX2g
Vgb4ZjtNgVzQ70kX4VNH04lqmkcnP8iY6dHHexwezls9KwNdouGVDSEFw6K0QOgu
T4s5nDtNADkNzaMXE11xL7ECgYBoML3rstFmTY1ymB0Uck3jtaP5jR+axdpt7weL
Zax4qooILhcXL6++DUhMAt5ecTOaPTzci7xKw/Xj3MLzZs8IV5R/WQhf2sj/+gEh
jy5UijwEaNmEO74dAkWPoMLsvGpocMzO8JeldnXNTXi+0noCgfvtgXnIMAQlnfMh
z0LviwKBgQCg5KR9JC4iuKses7Kfv2YelcO8vOZkRzBu3NdRWMsiJQC+qfetgd57
RjRjlRWd1WCHJ5Kmx3hkUaZZOrX5knqfsRW3Nl0I74xgWl7Bli2eSJ9VWl59bcd6
DqphhY7/gcW+QZlhXpnqbf0W8jB2gPhTYERyCBoS9LfhZWZu/11wuQ==
-----END RSA PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
MIICyTCCAjKgAwIBAgIBATANBgkqhkiG9w0BAQUFADBcMQswCQYDVQQGEwJHTzEM
MAoGA1UECBMDTUdPMQwwCgYDVQQHEwNNR08xDDAKBgNVBAoTA01HTzEPMA0GA1UE
CxMGU2VydmVyMRIwEAYDVQQDEwlsb2NhbGhvc3QwHhcNMTQwOTI0MTQwMzUzWhcN
MTUwOTI0MTQwMzUzWjBcMQswCQYDVQQGEwJHTzEMMAoGA1UECBMDTUdPMQwwCgYD
VQQHEwNNR08xDDAKBgNVBAoTA01HTzEPMA0GA1UECxMGQ2xpZW50MRIwEAYDVQQD
Ewlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDATayX
xh5NNJ63Cj2TKQnmYJn8W18+2f/Rfic6VMzZBz+5bMCyNJihJXhSS46JaMpBAfYQ
N5dqW2yE6jsXSgJjmjNV5B76mF73zuIpO8u1/s2hckMqo2pHgpvjKVsKsSTLv5e/
DrKuurDyqBbL71WyuKVHerwuwRMT1IEy1+ncFRpo1Cml42b6ICHHe5lK/eZ3NIsV
ID+jrWQDIbSxwsTFgOOj/vHIw1CiWgu82bHgijXrT7KU7PTvv+qpTrbS2a4MtHPV
B3Er9um2ZdOewEJHGX5TyfNvBVmBYUQ2FemVtw35KM0uF2e9OJTaoWGBmZztm0Ip
08inHdbqkf1vHuRPAgMBAAGjFzAVMBMGA1UdJQQMMAoGCCsGAQUFBwMCMA0GCSqG
SIb3DQEBBQUAA4GBAJZD7idSIRzhGlJYARPKWnX2CxD4VVB0F5cH5Mlc2YnoUSU/
rKuPZFuOYND3awKqez6K3rNb3+tQmNitmoOT8ImmX1uJKBo5w9tuo4B2MmLQcPMk
3fhPePuQCjtlArSmKVrNTrYPkyB9NwKS6q0+FzseFTw9ZJUIKiO9sSjMe+HP
-----END CERTIFICATE-----
`
	// The server cert is not used by the test. It is here for
	// convenience to use for the mongod specificed in the
	// `x509Flag` as the --sslPEMKeyFile and --sslCAFIle parameters.
	serverCert = `-----BEGIN CERTIFICATE-----
MIIC+DCCAmGgAwIBAgIJAJ5pBAq2HXAsMA0GCSqGSIb3DQEBBQUAMFwxCzAJBgNV
BAYTAkdPMQwwCgYDVQQIEwNNR08xDDAKBgNVBAcTA01HTzEMMAoGA1UEChMDTUdP
MQ8wDQYDVQQLEwZTZXJ2ZXIxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0xNDA5MjQx
MzUxMTBaFw0xNTA5MjQxMzUxMTBaMFwxCzAJBgNVBAYTAkdPMQwwCgYDVQQIEwNN
R08xDDAKBgNVBAcTA01HTzEMMAoGA1UEChMDTUdPMQ8wDQYDVQQLEwZTZXJ2ZXIx
EjAQBgNVBAMTCWxvY2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA
pQ5wO2L23xMI4PzpVt/Ftvez82IvA9amwr3fUd7RjlYwiFsFeMnG24a4CUoOeKF0
fpQWc9rmCs0EeP5ofZ2otOsfxoVWXZAZWdgauuwlYB6EeFaAMH3fxVH3IiH+21RR
q2w9sH/s4fqh5stavUfyPdVmCcb8NW0jD8jlqniJL0kCAwEAAaOBwTCBvjAdBgNV
HQ4EFgQUjyVWGMHBrmPDGwCY5VusHsKIpzIwgY4GA1UdIwSBhjCBg4AUjyVWGMHB
rmPDGwCY5VusHsKIpzKhYKReMFwxCzAJBgNVBAYTAkdPMQwwCgYDVQQIEwNNR08x
DDAKBgNVBAcTA01HTzEMMAoGA1UEChMDTUdPMQ8wDQYDVQQLEwZTZXJ2ZXIxEjAQ
BgNVBAMTCWxvY2FsaG9zdIIJAJ5pBAq2HXAsMAwGA1UdEwQFMAMBAf8wDQYJKoZI
hvcNAQEFBQADgYEAa65TgDKp3SRUDNAILSuQOCEbenWh/DMPL4vTVgo/Dxd4emoO
7i8/4HMTa0XeYIVbAsxO+dqtxqt32IcV7DurmQozdUZ7q0ueJRXon6APnCN0IqPC
sF71w63xXfpmnvTAfQXi7x6TUAyAQ2nScHExAjzc000DF1dO/6+nIINqNQE=
-----END CERTIFICATE-----
-----BEGIN RSA PRIVATE KEY-----
MIICWwIBAAKBgQClDnA7YvbfEwjg/OlW38W297PzYi8D1qbCvd9R3tGOVjCIWwV4
ycbbhrgJSg54oXR+lBZz2uYKzQR4/mh9nai06x/GhVZdkBlZ2Bq67CVgHoR4VoAw
fd/FUfciIf7bVFGrbD2wf+zh+qHmy1q9R/I91WYJxvw1bSMPyOWqeIkvSQIDAQAB
AoGABA9S22MXx2zkbwRJiQWAC3wURQxJM8L33xpkf9MHPIUKNJBolgwAhC3QIQpd
SMJP5z0lQDxGJEXesksvrsdN+vsgbleRfQsAIcY/rEhr9h8m6auM08f+69oIX32o
aTOWJJRofjbgzE5c/RijqhIaYGdq54a0EE9mAaODwZoa2/ECQQDRGrIRI5L3pdRA
yifDKNjvAFOk6TbdGe+J9zHFw4F7bA2In/b+rno9vrj+EanOevD8LRLzeFshzXrG
WQFzZ69/AkEAyhLSY7WNiQTeJWCwXawVnoSl5AMSRYFA/A2sEUokfORR5BS7gqvL
mmEKmvslnZp5qlMtM4AyrW2OaoGvE6sFNwJACB3xK5kl61cUli9Cu+CqCx0IIi6r
YonPMpvV4sdkD1ZycAtFmz1KoXr102b8IHfFQwS855aUcwt26Jwr4j70IQJAXv9+
PTXq9hF9xiCwiTkPaNh/jLQM8PQU8uoSjIZIpRZJkWpVxNay/z7D15xeULuAmxxD
UcThDjtFCrkw75Qk/QJAFfcM+5r31R1RrBGM1QPKwDqkFTGsFKnMWuS/pXyLTTOv
I+In9ZJyA/R5zKeJZjM7xtZs0ANU9HpOpgespq6CvA==
-----END RSA PRIVATE KEY-----`
)

func (s *S) TestAuthx509Cred(c *C) {
	if *x509Flag == "" {
		c.Skip("no -x509")
	}

	clientCert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientCert))
	c.Assert(err, IsNil)

	tlsConfig := &tls.Config{
		// Isolating tests to client certs, don't care about server validation.
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{clientCert},
	}

	c.Logf("Connecting to %s...", *x509Flag)
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs: []string{*x509Flag},
		DialServer: func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), tlsConfig)
		},
	})
	c.Assert(err, IsNil)
	defer session.Close()

	c.Logf("Connected! Testing the need for authentication...")
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
