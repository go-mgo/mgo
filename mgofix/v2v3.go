// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"go/ast"
	"strings"
)

func init() {
	register(v2v3Fix)
}

var v2v3Fix = fix{
	"v2v3",
	"2013-03-26",
	v2v3,
	`Fix incompatible changes from mgo v2 to v3

This fix rewrites code to fix the incompatibilities between
mgo v2 and v3: (*Iter).Err was renamed to (*Iter).Close.

http://codereview.appspot.com/5448065
`,
}

var iterTypeConfig = &TypeConfig{
	Func: map[string]string{
		"mgo.Dial": "(*mgo.Session, error)",
	},
	Type: map[string]*Type{
		"mgo.Session": {
			Method: map[string]string{
				"DB": "func() *mgo.Database",
			},
		},
		"mgo.Database": {
			Method: map[string]string{
				"C":       "func() *mgo.Collection",
				"FindRef": "func() *mgo.Query",
				"GridFS":  "func() *mgo.GridFS",
			},
		},
		"mgo.Collection": {
			Method: map[string]string{
				"Find":   "func() *mgo.Query",
				"FindId": "func() *mgo.Query",
				"With":   "func() *mgo.Collection",
				"Pipe":   "func() *mgo.Pipe",
			},
		},
		"mgo.GridFS": {
			Method: map[string]string{
				"Find": "func() *mgo.Query",
			},
		},
		"mgo.Query": {
			Method: map[string]string{
				"Iter":     "func() *mgo.Iter",
				"Tail":     "func() *mgo.Iter",
				"Batch":    "func() *mgo.Query",
				"Hint":     "func() *mgo.Query",
				"Limit":    "func() *mgo.Query",
				"Prefetch": "func() *mgo.Query",
				"Select":   "func() *mgo.Query",
				"Skip":     "func() *mgo.Query",
				"Snapshot": "func() *mgo.Query",
				"Sort":     "func() *mgo.Query",
			},
		},
		"mgo.Pipe": {
			Method: map[string]string{
				"Iter": "func() *mgo.Iter",
			},
		},
		"mgo.Iter": {},
	},
}

const v2path = "labix.org/v2/mgo"
const v3path = "labix.org/v3/mgo"

func v2v3(f *ast.File) bool {
	fixed := false
	for _, s := range f.Imports {
		old := importPath(s)
		if strings.HasPrefix(old, v2path) {
			new := v3path + old[len(v2path):]
			if rewriteImport(f, old, new) {
				fixed = true
			}
		}
	}
	if !fixed || !imports(f, v3path) {
		return fixed
	}
	typeof, _ := typecheck(iterTypeConfig, f)
	walk(f, func(n interface{}) {
		if call, ok := n.(*ast.CallExpr); ok {
			if len(call.Args) == 0 {
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if ok && sel.Sel.Name == "Err" && typeof[sel.X] == "*mgo.Iter" {
					sel.Sel.Name = "Close"
					fixed = true
				}
			}
			if len(call.Args) == 2 {
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if ok && sel.Sel.Name == "For" && (typeof[sel.X] == "*mgo.Iter" || typeof[sel.X] == "*mgo.Query") {
					warn(call.Pos(), "obsolete For method must be replaced by iterator; see labix.org/v2/mgo#Iter.Next")
				}
			}
		}
	})
	return fixed
}
