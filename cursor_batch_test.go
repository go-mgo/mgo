package mgo_test

import (
	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func (s *S) TestGetMore(c *C) {
	session, err := mgo.Dial("localhost:40001")
	session.SetBatch(4)
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	for i := 0; i < 10; i++ {
		coll.Insert(M{"n": i})
	}

	// Test random cursor should return error
	batch, exhauted, err := coll.CursorGetMore(12345, 123)
	c.Assert(err, ErrorMatches, "invalid cursor")

	// Test First Batch should return batch and cursor, no error
	batch, cursor, err := coll.Find(nil).CursorFirstBatch()
	c.Assert(len(batch), Equals, 4)
	c.Assert(cursor == 0, Equals, false)
	c.Assert(err == nil, Equals, true)

	// Test first call of GetMore should return batch and not exhausted yet, no error
	batch, exhauted, err = coll.CursorGetMore(cursor, 4)
	c.Assert(len(batch), Equals, 4)
	c.Assert(exhauted, Equals, false)
	c.Assert(err == nil, Equals, true)

	// Test second call of GetMore should return batch and exhausted, no error
	batch, exhauted, err = coll.CursorGetMore(cursor, 4)
	c.Assert(len(batch), Equals, 2)
	c.Assert(exhauted, Equals, true)
	c.Assert(err == nil, Equals, true)
}

func (s *S) TestCursorFirstBatchAndGetMore(c *C) {
	session, err := mgo.Dial("localhost:40001")
	c.Assert(err, IsNil)
	defer session.Close()

	coll := session.DB("mydb").C("mycoll")
	//Test the case db and coll doesn't exist
	batch, cursor, err := coll.Find(nil).Batch(5).CursorFirstBatch()
	c.Assert(len(batch), Equals, 0)
	c.Assert(cursor == 0, Equals, true)
	c.Assert(err, IsNil)

	for i := 0; i < 10; i++ {
		coll.Insert(M{"n": i})
	}

	//test the case get the first batch and still have document in coll
	batch, cursor, err = coll.Find(nil).Batch(5).Sort("n").CursorFirstBatch()
	iter := coll.Find(nil).Batch(5).Sort("n").Iter()
	c.Assert(err, IsNil)
	c.Assert(len(batch), Equals, 5)
	for i := 0; i < 5; i++ {
		itemRaw := batch[i]
		var item, itemNormal bson.M
		err := itemRaw.Unmarshal(&item)
		c.Assert(err, IsNil)
		ok := iter.Next(&itemNormal)
		c.Assert(ok, Equals, true)
		c.Assert(item, DeepEquals, itemNormal)
	}
	c.Assert(cursor == 0, Equals, false)

	// test the cursorid can be used to recreate iter
	batch, exhausted, err := coll.CursorGetMore(cursor, 5)
	c.Assert(err, IsNil)
	c.Assert(exhausted, Equals, false)
	c.Assert(len(batch), Equals, 5)
	for i := 0; i < 5; i++ {
		itemRaw := batch[i]
		var item, itemNormal bson.M
		err := itemRaw.Unmarshal(&item)
		c.Assert(err, IsNil)
		ok := iter.Next(&itemNormal)
		c.Assert(ok, Equals, true)
		c.Assert(item, DeepEquals, itemNormal)
	}
	batch, exhausted, err = coll.CursorGetMore(cursor, 5)
	c.Assert(err, IsNil)
	c.Assert(exhausted, Equals, true)
	c.Assert(len(batch), Equals, 0)

	//test the case the first batch exhaust the collection
	query := coll.Find(nil).Batch(10)
	batch, cursor, err = query.CursorFirstBatch()
	c.Assert(err, IsNil)
	c.Assert(len(batch), Equals, 10)
	c.Assert(cursor == 0, Equals, true)

	//test the case the first batch exhaust the collection
	query = coll.Find(nil).Batch(20)
	batch, cursor, err = query.CursorFirstBatch()
	c.Assert(err, IsNil)
	c.Assert(len(batch), Equals, 10)
	c.Assert(cursor == 0, Equals, true)
}
