// BSON library for Go
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
// gobson - BSON library for Go.
//

package bson

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

type bsonOptions struct {
	// skipCustom is used by
	//   - decode.go to skip looking for a custom SetBSON() or SetBSONWithContext()
	//   - encode.go to skip looking for a custom GetBSON() or GetBSONWithContext()
	// for any type with the base type name specified by 'skipCustom'.
	// This is useful to avoid infinite loop caused by:
	//    - calling Unmarshal from custom SetBSON function (decode.go)
	//    - encode.go calling custom GetBSON after just calling custom GetBSON for a given type
	skipCustom string
}

type key int

var bsonKey key = 0

// Returns the topmost bsonOptions value stored in ctx, if any.
func fromContext(ctx context.Context) (*bsonOptions, bool) {
	if ctx == nil {
		return nil, false
	}
	opts, ok := ctx.Value(bsonKey).(*bsonOptions)
	return opts, ok
}

// Returns the base type name (type name without a prefix that contains any combination of * or []).
func baseTypeName(typ reflect.Type) string {
	return strings.Trim(fmt.Sprintf("%v", typ), "*[]")
}

// Creates a new context with a value for skipCustom based on base type name of valu.
func NewContextWithSkipCustom(ctx context.Context, valu interface{}) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	//Debug(0, fmt.Sprintf("NewContextWithSkipCustom: %s", baseTypeName(reflect.TypeOf(valu))))
	return context.WithValue(ctx, bsonKey, &bsonOptions{skipCustom: baseTypeName(reflect.TypeOf(valu))})
}

// IsSkipCustom is useful to avoid infinite loop caused by:
//    - calling Unmarshal from custom SetBSON function (decode.go)
//    - encode.go calling custom GetBSON after just calling custom GetBSON for a given type
//
// Returns true if base type name of typ is the same as skipCustom.
//
// This method is used to skip all custom SetBSON/GetBSON functions of all types with the same base type.
// Note: if goal is to skip the custom functions of certain variations of a base type,
// skipCustom will not work (it will skip all variants).
func IsSkipCustom(ctx context.Context, typ reflect.Type) bool {
	if opts, _ := fromContext(ctx); opts != nil {
		//Debug(0, fmt.Sprintf("IsSkipCustom(%s) opts(%s) base(%s) %v", typ, opts.skipCustom,
		//	baseTypeName(typ), opts.skipCustom == baseTypeName(typ)))
		return opts.skipCustom == baseTypeName(typ)
	}
	//Debug(0, fmt.Sprintf("IsSkipCustom(%s) false", typ))
	return false
}

// These were useful in debugging infinite loop issues:
//    - calling Unmarshal from custom SetBSON function (decode.go)
//    - encode.go calling custom GetBSON after just calling custom GetBSON for a given type
// uncomment them (along with the invocations of Debug()) to aid debugging.
//func valueType(valu reflect.Value) string {
//	if !valu.IsValid() {
//		return "Invalid"
//	}
//	return fmt.Sprintf("%s", valu.Type())
//}
//
//var currentIndentLevel int = 0
//var infinteLoopCounter int = 10000
//
//func Debug(delta int, msg string) {
//	infinteLoopCounter--
//	if infinteLoopCounter < 0 {
//		panic(fmt.Errorf("YOU ARE IN INFINITE LOOP"))
//	}
//	if delta > 0 {
//		currentIndentLevel += delta
//	}
//	fmt.Printf("%s%s\n", strings.Repeat(" ", currentIndentLevel), msg)
//	if delta < 0 {
//		currentIndentLevel += delta
//	}
//}
