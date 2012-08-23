// The txn package implements support for multi-document transactions.
//
// For details check the following blog post:
//
//     http://blog.labix.org/2012/08/22/multi-doc-transactions-for-mongodb
//
package txn

import (
	"encoding/binary"
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"reflect"
	"sort"
	"sync"

	crand "crypto/rand"
	mrand "math/rand"
)

type state int

const (
	tpreparing state = 1 // One or more documents not prepared
	tprepared  state = 2 // Prepared but not yet ready to run
	taborting  state = 3 // Assertions failed, cleaning up
	tapplying  state = 4 // Changes are in progress
	taborted   state = 5 // Pre-conditions failed, nothing done
	tapplied   state = 6 // All changes applied 
)

var rand *mrand.Rand
var randmu sync.Mutex

func init() {
	var seed int64
	err := binary.Read(crand.Reader, binary.BigEndian, &seed)
	if err != nil {
		panic(err)
	}
	rand = mrand.New(mrand.NewSource(seed))
}

type transaction struct {
	Id     bson.ObjectId `bson:"_id"`
	State  state         `bson:"s"`
	Info   interface{}   `bson:"i,omitempty"`
	Ops    []Operation   `bson:"o"`
	Nonce  string        `bson:"n,omitempty"`
	Revnos []int64       `bson:"r,omitempty"`

	docKeysCached docKeys
}

func (t *transaction) String() string {
	if t.Nonce == "" {
		return t.Id.Hex()
	}
	return string(t.token())
}

func (t *transaction) done() bool {
	return t.State == tapplied || t.State == taborted
}

func (t *transaction) token() token {
	if t.Nonce == "" {
		panic("transaction has no nonce")
	}
	return tokenFor(t)
}

func (t *transaction) docKeys() docKeys {
	if t.docKeysCached != nil {
		return t.docKeysCached
	}
	dkeys := make(docKeys, 0, len(t.Ops))
NextOp:
	for _, op := range t.Ops {
		dkey := op.docKey()
		for i := range dkeys {
			if dkey == dkeys[i] {
				continue NextOp
			}
		}
		dkeys = append(dkeys, dkey)
	}
	sort.Sort(dkeys)
	t.docKeysCached = dkeys
	return dkeys
}

// tokenFor returns a unique transaction token that
// is composed by t's id and a nonce. If t already has
// a nonce assigned to it, it will be used, otherwise
// a new nonce will be generated.
func tokenFor(t *transaction) token {
	nonce := t.Nonce
	if nonce == "" {
		nonce = newNonce()
	}
	return token(t.Id.Hex() + "_" + nonce)
}

func newNonce() string {
	randmu.Lock()
	r := rand.Uint32()
	randmu.Unlock()
	n := make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		n[i] = "0123456789abcdef"[(r>>(4*i))&0xf]
	}
	return string(n)
}

type token string

func (tt token) id() bson.ObjectId { return bson.ObjectIdHex(string(tt[:24])) }
func (tt token) nonce() string     { return string(tt[25:]) }

// Operation represents a change to a single document that may be
// applied as part of a transaction with other operations.
type Operation struct {
	// Collection and DocId together identify the document this
	// operation refers to. DocId is matched against "_id".
	Collection string      `bson:"c"`
	DocId      interface{} `bson:"d"`

	// Assert optionally holds a query document that is used to
	// test the operation document at the time the transaction is
	// going to be applied. The assertions for all operations in
	// a transaction are tested before any changes take place,
	// and the transaction is entirely aborted if any of them
	// fails. This is also the only way to prevent a transaction
	// from being being applied (the transaction continues despite
	// the outcome of Insert, Update, and Remove).
	Assert interface{} `bson:"a,omitempty"`

	// The Insert, Update and Remove fields describe the mutation
	// intended by the operation. At most one of them may be set
	// per operation. If none are set, Assert must be set and the
	// operation becomes a read-only test.
	// 
	// Insert holds the document to be inserted at the time the
	// transaction is applied. The DocId field will be inserted
	// into the document automatically as its _id field. The
	// transaction will continue even if the document already
	// exists. Use Assert with txn.DocMissing if the insertion is
	// required.
	//
	// Update holds the update document to be applied at the time
	// the transaction is applied. The transaction will continue
	// even if a document with DocId is missing. Use Assert to
	// test for the document presence or its contents.
	//
	// Remove indicates whether to remove the document with DocId.
	// The transaction continues even if the document doesn't yet
	// exist at the time the transaction is applied. Use Assert
	// with txn.DocExists to make sure it will be removed.
	Insert interface{} `bson:"i,omitempty"`
	Update interface{} `bson:"u,omitempty"`
	Remove bool        `bson:"r,omitempty"`
}

func (op *Operation) isChange() bool {
	return op.Update != nil || op.Insert != nil || op.Remove
}

func (op *Operation) docKey() docKey {
	return docKey{op.Collection, op.DocId}
}

func (op *Operation) name() string {
	switch {
	case op.Update != nil:
		return "update"
	case op.Insert != nil:
		return "insert"
	case op.Remove:
		return "remove"
	case op.Assert != nil:
		return "assert"
	}
	return "none"
}

// A Runner applies operations as part of a transaction onto any number
// of collections within a database. See the Run method for details.
type Runner struct {
	tc *mgo.Collection
	sc *mgo.Collection
}

// NewRunner returns a new transaction runner that uses tc to hold its
// transactions.
//
// Multiple transaction collections may exist in a single database, but
// all collections that are touched by operations in a given transaction
// collection must be handled exclusively by it.
//
// A second collection with the same name of tc but suffixed by ".stash"
// will be used for implementing the transactional behavior of insert
// and remove operations.
func NewRunner(tc *mgo.Collection) *Runner {
	return &Runner{tc, tc.Database.C(tc.Name + ".stash")}
}

var ErrAborted = fmt.Errorf("transaction aborted")

// Run creates a new transaction with ops and runs it immediately.
// The id parameter specifies the transaction id, and may be written
// down ahead of time to later verify the success of the change and
// resume it, when the procedure is interrupted for any reason. If
// empty, a random id will be generated.
// The info parameter, if not nil, is included under the "i"
// field of the transaction document.
//
// Operations across documents are not atomically applied, but are
// guaranteed to be eventually all applied or all aborted, as long as
// the affected documents are only modified through transactions.
// If documents are simultaneously modified by transactions and out
// of transactions the behavior is undefined.
//
// If Run returns no errors, all operations were applied successfully.
// If it returns ErrAborted, one or more operations can't be applied
// and the transaction was entirely aborted with no changes performed.
// Otherwise, if the transaction is interrupted while running for any
// reason, it may be resumed explicitly or by attempting to apply
// another transaction on any of the documents targeted by ops, as
// long as the interruption was made after the transaction document
// itself was inserted. Run Resume with the obtained transaction id
// to confirm whether the transaction was applied or not.
//
// Any number of transactions may be run concurrently, with one
// runner or many.
func (r *Runner) Run(ops []Operation, id bson.ObjectId, info interface{}) (err error) {
	const efmt = "error in transaction op %d: %s"
	for i := range ops {
		op := &ops[i]
		if op.Collection == "" || op.DocId == nil {
			return fmt.Errorf(efmt, i, "Collection or DocId unset")
		}
		changes := 0
		if op.Insert != nil {
			changes++
		}
		if op.Update != nil {
			changes++
		}
		if op.Remove {
			changes++
		}
		if changes > 1 {
			return fmt.Errorf(efmt, i, "more than one of Insert/Update/Remove set")
		}
		if changes == 0 && op.Assert == nil {
			return fmt.Errorf(efmt, i, "none of Assert/Insert/Update/Remove set")
		}
	}
	if id == "" {
		id = bson.NewObjectId()
	}

	// Insert transaction sooner rather than later, to stay on the safer side.
	t := transaction{
		Id:    id,
		Ops:   ops,
		State: tpreparing,
		Info:  info,
	}
	if err = r.tc.Insert(&t); err != nil {
		return err
	}
	if err = flush(r, &t); err != nil {
		return err
	}
	if t.State == taborted {
		return ErrAborted
	} else if t.State != tapplied {
		panic(fmt.Errorf("invalid state for %s after flush: %q", &t, t.State))
	}
	return nil
}

// ResumeAll resumes all pending transactions. All ErrAborted errors
// from individual transactions are ignored.
func (r *Runner) ResumeAll() (err error) {
	iter := r.tc.Find(bson.D{{"state", bson.D{{"$in", []state{tpreparing, tprepared, tapplying}}}}}).Iter()
	var t transaction
	for iter.Next(&t) {
		if t.State == tapplied || t.State == taborted {
			continue
		}
		debugf("Resuming %s from %q", t.Id, t.State)
		if err := flush(r, &t); err != nil {
			return err
		}
		if !t.done() {
			panic(fmt.Errorf("invalid state for %s after flush: %q", &t, t.State))
		}
	}
	return nil
}

// Resume resumes the transaction with id. It returns mgo.ErrNotFound
// if the transaction is not found. Otherwise, it has the same semantics
// of the Run method after the transaction is inserted.
func (r *Runner) Resume(id bson.ObjectId) (err error) {
	t, err := r.load(id)
	if err != nil {
		return err
	}
	if !t.done() {
		debugf("Resuming %s from %q", t, t.State)
		if err := flush(r, t); err != nil {
			return err
		}
	}
	if t.State == taborted {
		return ErrAborted
	} else if t.State != tapplied {
		panic(fmt.Errorf("invalid state for %s after flush: %q", t, t.State))
	}
	return nil
}

func (r *Runner) load(id bson.ObjectId) (*transaction, error) {
	var t transaction
	err := r.tc.FindId(id).One(&t)
	if err == mgo.ErrNotFound {
		return nil, fmt.Errorf("cannot find transaction %s", id)
	} else if err != nil {
		return nil, err
	}
	return &t, nil
}

const (
	DocMissing = "missing"
	DocExists  = "exists"
)

type docKey struct {
	Collection string
	DocId      interface{}
}

type docKeys []docKey

func (ks docKeys) Len() int      { return len(ks) }
func (ks docKeys) Swap(i, j int) { ks[i], ks[j] = ks[j], ks[i] }
func (ks docKeys) Less(i, j int) bool {
	a, b := ks[i], ks[j]
	if a.Collection != b.Collection {
		return a.Collection < b.Collection
	}
	av, an := valueNature(a.DocId)
	bv, bn := valueNature(b.DocId)
	if an != bn {
		return an < bn
	}
	switch an {
	case natureString:
		return av.(string) < bv.(string)
	case natureInt:
		return av.(int64) < bv.(int64)
	case natureFloat:
		return av.(float64) < bv.(float64)
	case natureBool:
		return !av.(bool) && bv.(bool)
	}
	panic("unreachable")
}

type typeNature int

const (
	// The order of these values matters. Transactions
	// from applications using different ordering will
	// be incompatible with each other.
	_ typeNature = iota
	natureString
	natureInt
	natureFloat
	natureBool
)

func valueNature(v interface{}) (value interface{}, nature typeNature) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return rv.String(), natureString
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), natureInt
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), natureInt
	case reflect.Float32, reflect.Float64:
		return rv.Float(), natureFloat
	case reflect.Bool:
		return rv.Bool(), natureBool
	}
	panic("document id type unsupported by txn: " + rv.Kind().String())
}
