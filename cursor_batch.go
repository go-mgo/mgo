package mgo

import "gopkg.in/mgo.v2/bson"

// CursorGetMore gets a subsequent batch of documents for a given cursorID.
// Typically the cursorID is the one created using CursorFirstBatch. The
// numberToReturn indicates how many documents should be returned. This is the
// same as the "Batch" configuration on the Query, but it does not use the
// default session Batch and passes the value here to Mongo as is. The method
// returns the batch of documents, a boolean which is true if this is the last
// batch, and possibly an error.
func (c *Collection) CursorGetMore(cursor int64, numberToReturn int32) (batch []bson.Raw, exhausted bool, err error) {
	iter := c.NewIter(nil, nil, cursor, nil)
	iter.op.limit = numberToReturn
	batch, cursor, err = iter.getBatchDataAndCursor()
	return batch, cursor == 0, err
}

// CursorFirstBatch creates a new cursor and returns it's ID and the first
// batch of data. Use CursorGetMore to get subsequent batches of data.
func (q *Query) CursorFirstBatch() (batch []bson.Raw, cursorID int64, err error) {
	iter := q.Iter()
	return iter.getBatchDataAndCursor()
}

func (iter *Iter) getBatchDataAndCursor() ([]bson.Raw, int64, error) {
	var first bson.Raw
	if iter.err == nil {
		if !iter.Next(&first) {
			return nil, 0, iter.Err()
		}
	} else {
		return nil, 0, iter.Err()
	}

	results := make([]bson.Raw, 0, iter.docData.Len())
	results = append(results, first)
	for iter.docData.Len() != 0 {
		m := iter.docData.Pop()
		results = append(results, bson.Raw{
			Kind: 0x03,
			Data: m.([]byte),
		})
	}

	return results, iter.op.cursorId, nil
}
