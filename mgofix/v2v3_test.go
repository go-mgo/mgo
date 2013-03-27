package main

func init() {
	addTestCases(v2v3Tests, v2v3)
}

var v2v3Tests = []testCase{
	{
		Name: "v2v3.0",
		In: `package main

import "labix.org/v2/mgo"
import "labix.org/v2/mgo/bson"
import "labix.org/v2/mgo/txn"

func f() {
	session, _ := mgo.Dial("")
	c := session.DB("").C("")
	iter := c.Find(nil).Batch(2).Prefetch(0.5).Hint("").Iter()
	iter.Err()
}
`,
		Out: `package main

import "labix.org/v3/mgo"
import "labix.org/v3/mgo/bson"
import "labix.org/v3/mgo/txn"

func f() {
	session, _ := mgo.Dial("")
	c := session.DB("").C("")
	iter := c.Find(nil).Batch(2).Prefetch(0.5).Hint("").Iter()
	iter.Close()
}
`,
	},
}
