package bson_test

import (
	"gopkg.in/mgo.v2-unstable/bson"

	. "gopkg.in/check.v1"
	"reflect"
	"strings"
)

type jsonTest struct {
	a interface{}
	b string
	c interface{}
	e string
}

var jsonTests = []jsonTest{
	// $oid
	{
		a: bson.ObjectIdHex("0123456789abcdef01234567"),
		b: `{"$oid":"0123456789abcdef01234567"}`,
	}, {
		b: `ObjectId("0123456789abcdef01234567")`,
		c: bson.ObjectIdHex("0123456789abcdef01234567"),
	},

	// $ref (no special type)
	{
		b: `DBRef("name", "id")`,
		c: map[string]interface{}{"$ref": "name", "$id": "id"},
	},

	// $numberLong
	{
		a: 123,
		b: `123`,
	}, {
		a: int64(9007199254740992),
		b: `{"$numberLong":9007199254740992}`,
	}, {
		a: int64(1<<53 + 1),
		b: `{"$numberLong":"9007199254740993"}`,
	}, {
		a: 1<<53 + 1,
		b: `{"$numberLong":"9007199254740993"}`,
		c: int64(9007199254740993),
	}, {
		b: `NumberLong(9007199254740992)`,
		c: int64(1 << 53),
	}, {
		b: `NumberLong("9007199254740993")`,
		c: int64(1<<53 + 1),
	},

	// $minKey, $maxKey
	{
		a: bson.MinKey,
		b: `{"$minKey":1}`,
	}, {
		a: bson.MaxKey,
		b: `{"$maxKey":1}`,
	}, {
		b: `{"$minKey":0}`,
		e: `invalid $minKey object: {"$minKey":0}`,
	}, {
		b: `{"$maxKey":0}`,
		e: `invalid $maxKey object: {"$maxKey":0}`,
	},

	// $undefined
	{
		a: bson.Undefined,
		b: `{"$undefined":true}`,
	},
}

func (s *S) TestJSON(c *C) {
	for _, item := range jsonTests {
		c.Logf("------------")
		c.Logf("A: %#v", item.a)
		c.Logf("B: %#v", item.b)

		if item.c == nil {
			item.c = item.a
		} else {
			c.Logf("C: %#v", item.c)
		}
		if item.e != "" {
			c.Logf("E: %s", item.e)
		}

		if item.a != nil {
			data, err := bson.MarshalJSON(item.a)
			c.Assert(err, IsNil)
			c.Logf("Dumped: %#v", string(data))
			c.Assert(strings.TrimSuffix(string(data), "\n"), Equals, item.b)
		}

		var zero interface{}
		if item.c == nil {
			zero = &struct{}{}
		} else {
			zero = reflect.New(reflect.TypeOf(item.c)).Interface()
		}
		err := bson.UnmarshalJSON([]byte(item.b), zero)
		if item.e != "" {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, item.e)
			continue
		}
		c.Assert(err, IsNil)
		zerov := reflect.ValueOf(zero)
		value := zerov.Interface()
		if zerov.Kind() == reflect.Ptr {
			value = zerov.Elem().Interface()
		}
		c.Logf("Loaded: %#v", value)
		c.Assert(value, DeepEquals, item.c)
	}
}
