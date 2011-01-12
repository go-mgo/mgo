package mongogo_test

import (
    . "gocheck"
    "testing"
    "mongogo"
    "time"
    "os"
)


type cLogger C

func (c *cLogger) Output(calldepth int, s string) os.Error {
    ns := time.Nanoseconds()
    t := float64(ns % 100e9)/1e9
    ((*C)(c)).Logf("[LOG] %.05f %s", t, s)
    return nil
}


func TestAll(t *testing.T) {
    TestingT(t)
}

type S struct {
    session *mongogo.Session
}

var _ = Suite(&S{})

func (s *S) SetUpTest(c *C) {
    exec(c, "mongo --nodb testdb/dropall.js")
    mongogo.SetLogger((*cLogger)(c))
    mongogo.ResetStats()
}

func (s *S) SetUpSuite(c *C) {
    mongogo.Debug(true)
    mongogo.CollectStats(true)
}


func exec(c *C, command string) {
    name := "/bin/sh"
    argv := []string{name, "-c", command}
    envv := os.Environ()
    fdv := []*os.File{os.Stdin, nil, os.Stderr}

    pid, err := os.ForkExec(name, argv, envv, "", fdv)
    if err != nil {
        c.Fatalf("Failed to execute: %s: %s", command, err.String())
    }

    w, err := os.Wait(pid, 0)
    if err != nil {
        c.Fatalf("Error waiting for: %s: %s", command)
    }
    rc := w.ExitStatus()
    if rc != 0 {
        c.Fatalf("Process %s returned non-zero exit code (%d)", name, rc)
    }
}
