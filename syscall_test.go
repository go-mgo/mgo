// +build linux darwin

package mgo_test

import (
	"syscall"
)

func (s *S) Stop(host string) error {
	return syscall.Kill(s.pid(host), syscall.SIGSTOP)
}

func (s *S) Continue(host string) error {
	return syscall.Kill(s.pid(host), syscall.SIGCONT)
}
