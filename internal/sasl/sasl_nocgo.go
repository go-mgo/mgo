//go:build !cgo
// +build !cgo

package sasl

import "fmt"

type saslStepper interface {
	Step(serverData []byte) (clientData []byte, done bool, err error)
	Close()
}

func New(username, password, mechanism, service, host string) (saslStepper, error) {
	return nil, fmt.Errorf("SASL support requires cgo on this platform")
}
