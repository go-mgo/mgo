package bson

import (
	"fmt"
	"reflect"

	"gopkg.in/errgo.v1"
)

// Update represents a document update operation. When marshaled and
// provided to an update operation, it will set all the fields in Set
// and unset all the fields in Unset.
type Update struct {
	// Set holds the fields to be set keyed by field name.
	Set map[string]interface{} `bson:"$set,omitempty"`

	// Unset holds the fields to be unset keyed by field name. Note that
	// the key values will be ignored.
	Unset map[string]interface{} `bson:"$unset,omitempty"`
}

// AsUpdate returns the given object as an Update value holding all the
// fields of x, which must be acceptable to bson.Marshal, with
// zero-valued omitempty fields returned in Unset and others returned in
// Set. On success, the returned Set and Unset fields will always
// be non-nil, even when they contain no items.
//
// Note that the _id field is omitted, as it is not possible to set this
// in an update operation.
//
// This can be useful where an update operation is required to update
// only some subset of a given document without hard-coding all the
// struct fields into the update document.
//
// For example,
//
//	u, err := AsUpdate(x)
//	if err != nil {
//		...
//	}
//	coll.UpdateId(id, u)
//
// is equivalent to:
//
//	coll.UpdateId(id, x)
//
// as long as all the fields in the database document are
// mentioned in x. If there are other fields stored, they won't
// be affected.
func AsUpdate(x interface{}) (Update, error) {
	v := reflect.ValueOf(x)
	for {
		if vi, ok := v.Interface().(Getter); ok {
			getv, err := vi.GetBSON()
			if err != nil {
				return Update{}, fmt.Errorf("GetBSON failed: %v", err)
				panic(err)
			}
			v = reflect.ValueOf(getv)
			continue
		}
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
			continue
		}
		break
	}
	if v.Type() == typeRaw {
		return nonStructAsUpdate(v.Addr())
	}
	var u Update
	var err error
	switch t := v.Type(); t.Kind() {
	case reflect.Map:
		u, err = mapAsUpdate(v)
	case reflect.Struct:
		u, err = structAsUpdate(v)
	default:
		u, err = nonStructAsUpdate(v)
	}
	delete(u.Set, "_id")
	delete(u.Unset, "_id")
	return u, err
}

func structAsUpdate(v reflect.Value) (Update, error) {
	sinfo, err := getStructInfo(v.Type())
	if err != nil {
		return Update{}, err
	}
	u := Update{
		Set:   make(M),
		Unset: make(M),
	}
	if sinfo.InlineMap >= 0 {
		if m := v.Field(sinfo.InlineMap); m.Len() != 0 {
			for _, k := range m.MapKeys() {
				ks := k.String()
				if _, found := sinfo.FieldsMap[ks]; found {
					return Update{}, errgo.Newf("Can't have key %q in inlined map; conflicts with struct field", ks)
				}
				if ks != "_id" {
					u.Set[ks] = m.MapIndex(k).Interface()
				}
			}
		}
	}
	var value reflect.Value
	for _, info := range sinfo.FieldsList {
		if info.Key == "_id" {
			continue
		}
		if info.Inline == nil {
			value = v.Field(info.Num)
		} else {
			value = v.FieldByIndex(info.Inline)
		}
		if info.OmitEmpty && isZero(value) {
			u.Unset[info.Key] = nil
		} else {
			u.Set[info.Key] = value.Interface()
		}
	}
	return u, nil
}

func nonStructAsUpdate(v reflect.Value) (Update, error) {
	var m map[string]Raw
	data, err := Marshal(v.Interface())
	if err != nil {
		return Update{}, errgo.Notef(err, "cannot marshal")
	}
	if err := Unmarshal(data, &m); err != nil {
		return Update{}, err
	}
	return mapAsUpdate(reflect.ValueOf(m))
}

func mapAsUpdate(v reflect.Value) (Update, error) {
	if v.Type().Key().Kind() != reflect.String {
		return Update{}, errgo.Newf("map key not a string")
	}
	u := Update{
		Set:   make(M),
		Unset: make(M),
	}
	for _, k := range v.MapKeys() {
		ks := k.String()
		if ks == "_id" {
			continue
		}
		u.Set[ks] = v.MapIndex(k).Interface()
	}
	return u, nil
}
