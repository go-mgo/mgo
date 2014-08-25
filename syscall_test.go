// +build linux darwin

package mgo_test

import (
	"syscall"
)

func (s *S) Freeze(host string) {
	err := syscall.Kill(s.pid(host), syscall.SIGSTOP)
	if err != nil {
		panic(err)
	}
	s.frozen = append(s.frozen, host)
}

func (s *S) Thaw(host string) {
	err := syscall.Kill(s.pid(host), syscall.SIGCONT)
	if err != nil {
		panic(err)
	}
	for i, frozen := range s.frozen {
		if frozen == host {
			s.frozen[i] = ""
		}
	}
}
