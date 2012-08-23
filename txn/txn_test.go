package txn_test

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"labix.org/v2/mgo/txn"
	. "launchpad.net/gocheck"
	"testing"
)

func TestAll(t *testing.T) {
	TestingT(t)
}

type S struct {
	MgoSuite

	db       *mgo.Database
	tc, sc   *mgo.Collection
	accounts *mgo.Collection
	runner   *txn.Runner
}

var _ = Suite(&S{})

type M map[string]interface{}

func (s *S) SetUpTest(c *C) {
	txn.SetChaos(txn.Chaos{})
	txn.SetLogger(c)
	txn.SetDebug(true)
	s.MgoSuite.SetUpTest(c)

	s.db = s.session.DB("test")
	s.tc = s.db.C("tc")
	s.sc = s.db.C("tc.stash")
	s.accounts = s.db.C("accounts")
	s.runner = txn.NewRunner(s.tc)
}

type Account struct {
	Id      int `bson:"_id"`
	Balance int
}

func (s *S) TestDocExists(c *C) {
	err := s.accounts.Insert(M{"_id": 0, "balance": 300})
	c.Assert(err, IsNil)

	exists := []txn.Operation{{
		Collection: "accounts",
		DocId:      0,
		Assert:     txn.DocExists,
	}}
	missing := []txn.Operation{{
		Collection: "accounts",
		DocId:      0,
		Assert:     txn.DocMissing,
	}}

	err = s.runner.Run(exists, "", nil)
	c.Assert(err, IsNil)
	err = s.runner.Run(missing, "", nil)
	c.Assert(err, Equals, txn.ErrAborted)

	err = s.accounts.RemoveId(0)
	c.Assert(err, IsNil)

	err = s.runner.Run(exists, "", nil)
	c.Assert(err, Equals, txn.ErrAborted)
	err = s.runner.Run(missing, "", nil)
	c.Assert(err, IsNil)
}

func (s *S) TestInsert(c *C) {
	err := s.accounts.Insert(M{"_id": 0, "balance": 300})
	c.Assert(err, IsNil)

	ops := []txn.Operation{{
		Collection: "accounts",
		DocId:      0,
		Insert:     M{"balance": 200},
	}}

	err = s.runner.Run(ops, "", nil)
	c.Assert(err, IsNil)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 300)

	ops[0].DocId = 1
	err = s.runner.Run(ops, "", nil)
	c.Assert(err, IsNil)

	err = s.accounts.FindId(1).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 200)
}

func (s *S) TestRemove(c *C) {
	err := s.accounts.Insert(M{"_id": 0, "balance": 300})
	c.Assert(err, IsNil)

	ops := []txn.Operation{{
		Collection: "accounts",
		DocId:      0,
		Remove:     true,
	}}

	err = s.runner.Run(ops, "", nil)
	c.Assert(err, IsNil)

	err = s.accounts.FindId(0).One(nil)
	c.Assert(err, Equals, mgo.ErrNotFound)

	err = s.runner.Run(ops, "", nil)
	c.Assert(err, IsNil)
}

func (s *S) TestQueueStashing(c *C) {
	txn.SetChaos(txn.Chaos{
		KillChance: 1,
		Breakpoint: "checkpoint",
	})

	opses := [][]txn.Operation{{{
		Collection: "accounts",
		DocId:      0,
		Insert:     M{"balance": 100},
	}}, {{
		Collection: "accounts",
		DocId:      0,
		Remove:     true,
	}}, {{
		Collection: "accounts",
		DocId:      0,
		Insert:     M{"balance": 200},
	}}, {{
		Collection: "accounts",
		DocId:      0,
		Update:     M{"$inc": M{"balance": 100}},
	}}}

	var last bson.ObjectId
	for _, ops := range opses {
		last = bson.NewObjectId()
		err := s.runner.Run(ops, last, nil)
		c.Assert(err, Equals, txn.ErrChaos)
	}

	txn.SetChaos(txn.Chaos{})
	err := s.runner.Resume(last)
	c.Assert(err, IsNil)

	var account Account
	err = s.accounts.FindId(0).One(&account)
	c.Assert(err, IsNil)
	c.Assert(account.Balance, Equals, 300)
}

func (s *S) TestInfo(c *C) {
	ops := []txn.Operation{{
		Collection: "accounts",
		DocId:      0,
		Assert:     txn.DocMissing,
	}}

	id := bson.NewObjectId()
	err := s.runner.Run(ops, id, M{"n": 42})
	c.Assert(err, IsNil)

	var t struct{ I struct{ N int } }
	err = s.tc.FindId(id).One(&t)
	c.Assert(err, IsNil)
	c.Assert(t.I.N, Equals, 42)
}

func (s *S) TestErrors(c *C) {
	doc := bson.M{"foo": 1}
	tests := []txn.Operation{{
		Collection: "c",
		DocId: 0,
	}, {
		Collection: "c",
		DocId: 0,
		Insert: doc,
		Remove: true,
	}, {
		Collection: "c",
		DocId: 0,
		Insert: doc,
		Update: doc,
	}, {
		Collection: "c",
		DocId: 0,
		Update: doc,
		Remove: true,
	}, {
		Collection: "c",
		Assert: doc,
	}, {
		DocId: 0,
		Assert: doc,
	}}

	txn.SetChaos(txn.Chaos{KillChance: 1.0})
	for _, op := range tests {
		c.Logf("op: %v", op)
		err := s.runner.Run([]txn.Operation{op}, "", nil)
		c.Assert(err, ErrorMatches, "error in transaction op 0: .*")
	}
}
