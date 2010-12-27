package mongogo_test


import (
    . "gocheck"
    b "gobson"
    "mongogo"
)


func (s *S) TestBasicInsertAndFind(c *C) {
    session, err := mongogo.Mongo("localhost:40001")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(b.M{"a": 1, "b": 2})
    value, err := coll.Find(b.M{"a": 1}).One()
    c.Assert(err, IsNil)
    c.Assert(value, NotNil)
    c.Assert(value["a"], Equals, int32(1))
    c.Assert(value["b"], Equals, int32(2))

    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 0)
}

func (s *S) TestInsertAndFindWithSlaveConnection(c *C) {
    session, err := mongogo.Mongo("localhost:40012")
    c.Assert(err, IsNil)

    coll := session.DB("mydb").C("mycollection")
    coll.Insert(b.M{"a": 1, "b": 2})
    value, err := coll.Find(b.M{"a": 1}).One()
    c.Assert(err, IsNil)
    c.Assert(value, NotNil)
    c.Assert(value["a"], Equals, int32(1))
    c.Assert(value["b"], Equals, int32(2))

    stats := mongogo.GetStats()
    c.Assert(stats.MasterConns, Equals, 1)
    c.Assert(stats.SlaveConns, Equals, 2)
}
