// +build windows

package mgo_test

func (s *S) Stop(host string) {
	panic("Stop() not (currently) supported on Windows!")
}

func (s *S) Continue(host string) {
	panic("Continue() not (currently) supported on Windows!")
}
