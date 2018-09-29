package mgo

import "fmt"

func ExampleQueryError() {
	e := &QueryError{
		Code:      17406,
		Message:   "getMore executor error: CappedPositionLost: CollectionScan died due to failure to restore tailable cursor position. Last seen record id: RecordId(200138)",
		Assertion: false,
	}
	fmt.Println(e.String())
	// Output:
	// getMore executor error: CappedPositionLost: CollectionScan died due to failure to restore tailable cursor position. Last seen record id: RecordId(200138)
}
