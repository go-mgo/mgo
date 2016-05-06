package bson

import (
	"bytes"
	"fmt"
	"gopkg.in/mgo.v2-unstable/internal/json"
)

func UnmarshalJSON(data []byte, value interface{}) error {
	d := json.NewDecoder(bytes.NewBuffer(data))
	d.Extend(&jsonExt)
	return d.Decode(value)
}

func MarshalJSON(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	e.Extend(&jsonExt)
	err := e.Encode(value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// jdec is used internally by the JSON decoding functions
// so they may unmarshal functions without getting into endless
// recursion due to keyed objects.
func jdec(data []byte, value interface{}) error {
	d := json.NewDecoder(bytes.NewBuffer(data))
	d.Extend(&funcExt)
	return d.Decode(value)
}

var jsonExt json.Extension
var funcExt json.Extension

func init() {
	funcExt.DecodeFunc("ObjectId", "$oidFunc", "Id")
	jsonExt.DecodeKeyed("$oid", jdecObjectId)
	jsonExt.DecodeKeyed("$oidFunc", jdecObjectId)
	jsonExt.EncodeType(ObjectId(""), jencObjectId)

	funcExt.DecodeFunc("DBRef", "$dbrefFunc", "$ref", "$id")
	jsonExt.DecodeKeyed("$dbrefFunc", jdecDBRef)

	funcExt.DecodeFunc("NumberLong", "$numberLongFunc", "N")
	jsonExt.DecodeKeyed("$numberLong", jdecNumberLong)
	jsonExt.DecodeKeyed("$numberLongFunc", jdecNumberLong)
	jsonExt.EncodeType(int64(0), jencNumberLong)
	jsonExt.EncodeType(int(0), jencInt)

	jsonExt.DecodeKeyed("$minKey", jdecMinKey)
	jsonExt.DecodeKeyed("$maxKey", jdecMaxKey)
	jsonExt.EncodeType(orderKey(0), jencMinMaxKey)

	jsonExt.DecodeKeyed("$undefined", jdecUndefined)
	jsonExt.EncodeType(Undefined, jencUndefined)

	jsonExt.Extend(&funcExt)
}

func fbytes(format string, args ...interface{}) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, format, args...)
	return buf.Bytes()
}

func jdecObjectId(data []byte) (interface{}, error) {
	println("Here!")
	var v struct {
		Id   string `json:"$oid"`
		Func struct {
			Id string
		} `json:"$oidFunc"`
	}
	err := jdec(data, &v)
	if err != nil {
		return nil, err
	}
	if v.Id == "" {
		v.Id = v.Func.Id
	}
	return ObjectIdHex(v.Id), nil
}

func jencObjectId(v interface{}) ([]byte, error) {
	return fbytes(`{"$oid":"%s"}`, v.(ObjectId).Hex()), nil
}

func jdecDBRef(data []byte) (interface{}, error) {
	// TODO Support unmarshaling $ref and $id into the input value.
	var v struct {
		Obj map[string]interface{} `json:"$dbrefFunc"`
	}
	// TODO Fix this. Must not be required.
	v.Obj = make(map[string]interface{})
	err := jdec(data, &v)
	if err != nil {
		return nil, err
	}
	return v.Obj, nil
}

func jdecNumberLong(data []byte) (interface{}, error) {
	var v struct {
		N    int64 `json:"$numberLong,string"`
		Func struct {
			N int64 `json:",string"`
		} `json:"$numberLongFunc"`
	}
	var vn struct {
		N    int64 `json:"$numberLong"`
		Func struct {
			N int64
		} `json:"$numberLongFunc"`
	}
	err := jdec(data, &v)
	if err != nil {
		err = jdec(data, &vn)
		v.N = vn.N
		v.Func.N = vn.Func.N
	}
	if err != nil {
		return nil, err
	}
	if v.N != 0 {
		return v.N, nil
	}
	return v.Func.N, nil
}

func jencNumberLong(v interface{}) ([]byte, error) {
	n := v.(int64)
	f := `{"$numberLong":"%d"}`
	if n <= 1<<53 {
		f = `{"$numberLong":%d}`
	}
	return fbytes(f, n), nil
}

func jencInt(v interface{}) ([]byte, error) {
	n := v.(int)
	f := `{"$numberLong":"%d"}`
	if n <= 1<<53 {
		f = `%d`
	}
	return fbytes(f, n), nil
}

func jdecMinKey(data []byte) (interface{}, error) {
	var v struct {
		N int64 `json:"$minKey"`
	}
	err := jdec(data, &v)
	if err != nil {
		return nil, err
	}
	if v.N != 1 {
		return nil, fmt.Errorf("invalid $minKey object: %s", data)
	}
	return MinKey, nil
}

func jdecMaxKey(data []byte) (interface{}, error) {
	var v struct {
		N int64 `json:"$maxKey"`
	}
	err := jdec(data, &v)
	if err != nil {
		return nil, err
	}
	if v.N != 1 {
		return nil, fmt.Errorf("invalid $maxKey object: %s", data)
	}
	return MaxKey, nil
}

func jencMinMaxKey(v interface{}) ([]byte, error) {
	switch v.(orderKey) {
	case MinKey:
		return []byte(`{"$minKey":1}`), nil
	case MaxKey:
		return []byte(`{"$maxKey":1}`), nil
	}
	panic(fmt.Sprintf("invalid $minKey/$maxKey value: %d", v))
}

func jdecUndefined(data []byte) (interface{}, error) {
	var v struct {
		B bool `json:"$undefined"`
	}
	err := jdec(data, &v)
	if err != nil {
		return nil, err
	}
	if !v.B {
		return nil, fmt.Errorf("invalid $undefined object: %s", data)
	}
	return Undefined, nil
}

func jencUndefined(v interface{}) ([]byte, error) {
	return []byte(`{"$undefined":true}`), nil
}
