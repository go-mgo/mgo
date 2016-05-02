package json

// Extension holds a set of additional rules to be used when unmarshaling
// strict JSON or JSON-like content.
type Extension struct {
	funcs   map[string]funcExt
	keyed   map[string]func([]byte) interface{}
	replace []func(value interface{}) interface{}
}

type funcExt struct {
	key  string
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
// JSON document triggers the decoding of that document via the provided
// decode function.
func (e *Extension) KeyedDoc(key string, decode func(data []byte) interface{}) {
	if e.keyed == nil {
		e.keyed = make(map[string]func([]byte) interface{})
	}
	e.keyed[key] = decode
}

// PreEncode registers the replace function to be called before any value is encoded.
// The returned value will be encoded in place of the original value.
func (e *Extension) PreEncode(replace func(value interface{}) interface{}) {
	e.replace = append(e.replace, replace)
}
