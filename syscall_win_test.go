// +build windows

package mgo_test

func (s *S) Freeze(host string) {
	panic("Freeze() not (currently) supported on Windows!")
}

func (s *S) Thaw(host string) {
	panic("Thaw() not (currently) supported on Windows!")
}
