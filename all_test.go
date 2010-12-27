package mongogo_test


import (
    . "gocheck"
    B "gobson"
    "mongogo"
)


// Connect to the master of a deployment with a single server,
// run an insert, and then ensure the insert worked and that a
// single connection was established.
func (s *S) TestTopologySyncWithSingleMaster(c *C) {
    // Use hostname here rather than IP, to make things trickier.
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(B.M{"a": 1, "b": 2})

    result := struct{Ok bool}{}
    err = session.Run("getLastError", &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, true)

    // One connection during discovery. Master socket
    // recycled for insert. 
    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 0)
}

func (s *S) TestTopologySyncWithSlaveSeed(c *C) {
    // That's supposed to be a slave. Must run discovery
    // and find out master to insert successfully.
    session, err := mongogo.Mongo("localhost:40012")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(B.M{"a": 1, "b": 2})

    result := struct{Ok bool}{}
    err = session.Run("getLastError", &result)
    c.Assert(err, IsNil)
    c.Assert(result.Ok, Equals, true)

    // One connection to each during discovery. Master
    // socket recycled for insert. 
    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 2)
}

func (s *S) TestInsertFindOneM(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(B.M{"a": 1, "b": 2})
    value, err := coll.Find(B.M{"a": 1}).OneM()
    c.Assert(err, IsNil)
    c.Assert(value, NotNil)
    c.Assert(value["a"], Equals, int32(1))
    c.Assert(value["b"], Equals, int32(2))
}

func (s *S) TestInsertFindOne(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(B.M{"a": 1, "b": 2})

    doc := struct{A, B int}{}

    err = coll.Find(B.M{"a": 1}).One(&doc)
    c.Assert(err, IsNil)
    c.Assert(doc.A, Equals, 1)
    c.Assert(doc.B, Equals, 2)
}

func (s *S) TestRunMString(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    doc, err := session.RunM("ping")
    c.Assert(err, IsNil)
    c.Assert(doc, Equals, B.M{"ok": float64(1)})
}

func (s *S) TestRunMValue(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    doc, err := session.RunM(B.M{"ping": 1})
    c.Assert(err, IsNil)
    c.Assert(doc, Equals, B.M{"ok": float64(1)})
}

func (s *S) TestRunString(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    doc := struct{Ok int}{}

    err = session.Run("ping", &doc)
    c.Assert(err, IsNil)
    c.Assert(doc.Ok, Equals, 1)
}

func (s *S) TestRunValue(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    doc := struct{Ok int}{}

    err = session.Run(B.M{"ping": 1}, &doc)
    c.Assert(err, IsNil)
    c.Assert(doc.Ok, Equals, 1)
}
