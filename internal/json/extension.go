package json

// Extension holds a set of additional rules to be used when unmarshaling
// strict JSON or JSON-like content.
type Extension struct {
	funcs map[string]funcExt
	keyed map[string]func() interface{}
}

type funcExt struct {
	key string
	args []string
}

// Extend changes the decoder behavior to consider the provided extension.
func (dec *Decoder) Extend(ext *Extension) { dec.d.ext = ext }

// Func defines a function call that may be observed inside JSON content.
// A function with the provided name will be unmarshaled as the document
// {key: {args[0]: ..., args[N]: ...}}.
func (e *Extension) Func(name string, key string, args ...string) {
	if e.funcs == nil {
		e.funcs = make(map[string]funcExt)
	}
	e.funcs[name] = funcExt{key, args}
}

// KeyedDoc defines a key that when observed as the first element inside a
// JSON document or sub-document triggers the parsing of that document as
// the value returned by the provided function.
func (e *Extension) KeyedDoc(key string, new func() interface{}) {
	if e.keyed == nil {
		e.keyed = make(map[string]func() interface{})
	}
	e.keyed[key] = new
}
