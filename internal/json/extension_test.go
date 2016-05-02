package json

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

type funcN struct {
	Arg1 int `json:"arg1"`
	Arg2 int `json:"arg2"`
}

type funcs struct {
	Func2 *funcN `json:"$func2"`
	Func1 *funcN `json:"$func1"`
}

var ext Extension

func init() {
	ext.Func("Func1", "$func1")
	ext.Func("Func2", "$func2", "arg1", "arg2")
}

type extensionTest struct {
	in        string
	ptr       interface{}
	out       interface{}
	err       error
}

var extensionTests = []extensionTest{
	{in: `Func1()`, ptr: new(interface{}), out: map[string]interface{}{
		"$func1": map[string]interface{}{},
	}},
	{in: `Func2(1)`, ptr: new(interface{}), out: map[string]interface{}{
		"$func2": map[string]interface{}{"arg1": float64(1)},
	}},
	{in: `Func2(1, 2)`, ptr: new(interface{}), out: map[string]interface{}{
		"$func2": map[string]interface{}{"arg1": float64(1), "arg2": float64(2)},
	}},
	{in: `Func2(Func1())`, ptr: new(interface{}), out: map[string]interface{}{
		"$func2": map[string]interface{}{"arg1": map[string]interface{}{"$func1": map[string]interface{}{}}},
	}},
	{in: `Func2(1, 2, 3)`, ptr: new(interface{}), err: fmt.Errorf("json: too many arguments for function Func2")},
	{in: `Func3()`, ptr: new(interface{}), err: fmt.Errorf("json: unknown function Func3")},

	{in: `Func1()`, ptr: new(funcs), out: funcs{Func1: &funcN{}}},
	{in: `Func2(1)`, ptr: new(funcs), out: funcs{Func2: &funcN{Arg1: 1}}},
	{in: `Func2(1, 2)`, ptr: new(funcs), out: funcs{Func2: &funcN{Arg1: 1, Arg2: 2}}},

	{in: `Func2(1, 2, 3)`, ptr: new(funcs), err: fmt.Errorf("json: too many arguments for function Func2")},
	{in: `Func3()`, ptr: new(funcs), err: fmt.Errorf("json: unknown function Func3")},
}

func TestExtensions(t *testing.T) {
	for i, tt := range extensionTests {
		var scan scanner
		in := []byte(tt.in)
		if err := checkValid(in, &scan); err != nil {
			if !reflect.DeepEqual(err, tt.err) {
				t.Errorf("#%d: checkValid: %#v", i, err)
				continue
			}
		}
		if tt.ptr == nil {
			continue
		}

		// v = new(right-type)
		v := reflect.New(reflect.TypeOf(tt.ptr).Elem())
		dec := NewDecoder(bytes.NewReader(in))
		dec.Extend(&ext)
		if err := dec.Decode(v.Interface()); !reflect.DeepEqual(err, tt.err) {
			t.Errorf("#%d: %v, want %v", i, err, tt.err)
			continue
		} else if err != nil {
			continue
		}
		if !reflect.DeepEqual(v.Elem().Interface(), tt.out) {
			t.Errorf("#%d: mismatch\nhave: %#+v\nwant: %#+v", i, v.Elem().Interface(), tt.out)
			data, _ := Marshal(v.Elem().Interface())
			t.Logf("%s", string(data))
			data, _ = Marshal(tt.out)
			t.Logf("%s", string(data))
			continue
		}
	}
}
