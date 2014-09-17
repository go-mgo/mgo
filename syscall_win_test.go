// +build windows

package mgo_test

func stop(pid int) (err error) {
	panic("stop not currently implemented on windows!")
}

func cont(pid int) (err error) {
	panic("cont not currently implemented on windows!")
}
