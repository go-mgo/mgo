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

type state string

const (
	tpreparing state = "preparing" // One or more documents not prepared
	tprepared  state = "prepared"  // Prepared but not yet ready to run
	tapplying  state = "applying"  // Changes are in progress
	taborting  state = "aborting"  // Assertions failed, cleaning up
	tapplied   state = "applied"   // All changes applied 
	taborted   state = "aborted"   // Pre-conditions failed, nothing done
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
	State  state
	Info   interface{} `bson:",omitempty"`
	Ops    []Operation
	Nonce  string  `bson:",omitempty"`
	Revnos []int64 `bson:",omitempty"`
	//MTime  time.Time

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
	Collection string
	DocId      interface{}

	// Assert optionally holds a query document that is used to
	// test the operation document at the time the transaction is
	// going to be applied. The assertions for all operatoins in
	// a transaction are tested before any changes take place,
	// and the transaction is entirely aborted if any of them
	// fails. This is also the only way to prevent a transaction
	// from being being applied (the transaction continues despite
	// the outcome of Insert, Remove and Change).
	Assert interface{}

	// The Insert, Change and Remove fields describe the mutation
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
	// Change holds the change document to be applied at the time
	// the transaction is applied. The transaction will continue
	// even if a document with DocId is missing. Use Assert to
	// test for the document presence or its contents.
	//
	// Remove indicates whether to remove the document with DocId.
	// The transaction continues even if the document doesn't yet
	// exist at the time the transaction is applied. Use Assert
	// with txn.DocExists to make sure it will be removed.
	Insert interface{}
	Change interface{}
	Remove bool
}

func (op *Operation) isChange() bool {
	return op.Change != nil || op.Insert != nil || op.Remove
}

func (op *Operation) docKey() docKey {
	return docKey{op.Collection, op.DocId}
}

func (op *Operation) name() string {
	switch {
	case op.Change != nil:
		return "change"
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

	fake bool
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
	return &Runner{tc, tc.Database.C(tc.Name + ".stash"), false}
}

// NewFakeRunner returns a new transaction runner that doesn't present
// real transactional behavior. It runs operations serially as they
// would have been normally executed. This may be useful in tests,
// performance comparisons, or to easily shift code from transactional
// behavior to straight logic.
func NewFakeRunner(tc *mgo.Collection) *Runner {
	return &Runner{tc, tc.Database.C(tc.Name + ".stash"), true}
}

var ErrAborted = fmt.Errorf("transaction aborted")

// Run creates a new transaction with ops and runs it immediately.
// The id parameter specifies the transaction id, and may be written
// down ahead of time to later verify the success of the change and
// resume it, when the procedure is interrupted for any reason. If
// empty, a random id will be generated.
// The info parameter, if not nil, is included under the "info"
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
	if r.fake {
		return r.fakeRun(ops)
	}

	if id == "" {
		id = bson.NewObjectId()
	}

	// Insert transaction sooner rather than later, to stay on the safer side.
	t := transaction{
		Id:    id,
		Ops:   ops,
		State: tpreparing,
		// Mtime
		// Info
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

// ResumeAll resumes all pending transactions. An error is only returned
// if resuming is somehow interrupted.
func (r *Runner) ResumeAll() (err error) {
	if r.fake {
		return nil
	}
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
	if r.fake {
		return nil
	}
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

func (r *Runner) fakeRun(ops []Operation) error {
	qdoc := make(bson.D, 2)

	// Verify that the requested assertions match.
	for _, op := range ops {
		if op.Assert == nil {
			continue
		}
		if op.Insert != nil && op.Assert != DocMissing {
			return fmt.Errorf("Insert can only Assert txn.DocMissing")
		}

		c := r.tc.Database.C(op.Collection)
		qdoc = append(qdoc[:0], bson.DocElem{"_id", op.DocId})
		if op.Insert == nil && op.Assert != DocExists && op.Assert != DocMissing {
			qdoc = append(qdoc, bson.DocElem{"$or", []interface{}{op.Assert}})
		}
		err := c.Find(qdoc).Select(bson.D{{"_id", 1}}).One(nil)
		if err != nil && err != mgo.ErrNotFound {
			return err
		}
		if (err == mgo.ErrNotFound) != (op.Assert == DocMissing) {
			return ErrAborted
		}
	}

	// Apply the changes.
	for _, op := range ops {
		c := r.tc.Database.C(op.Collection)

		qdoc = append(qdoc[:0], bson.DocElem{"_id", op.DocId})

		var err error
		switch {
		case op.Change != nil:
			err = c.Update(qdoc, op.Change)
			if err == mgo.ErrNotFound {
				err = nil
			}
		case op.Insert != nil:
			err = c.Insert(op.Insert)
			if lerr, ok := err.(*mgo.LastError); ok && lerr.Code == 11000 {
				err = nil
			}
		case op.Remove:
			err = c.Remove(qdoc)
			if err == mgo.ErrNotFound {
				err = nil
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

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
