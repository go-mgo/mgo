package mongogo_test

import (
    .   "gocheck"
    "testing"
    "mongogo"
    "time"
    "fmt"
    "os"
)


type cLogger C

func (c *cLogger) Output(calldepth int, s string) os.Error {
    ns := time.Nanoseconds()
    t := float64(ns%100e9) / 1e9
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
    err := exec("mongo --nodb testdb/dropall.js")
    if err != nil {
        panic(err.String())
    }
    mongogo.SetLogger((*cLogger)(c))
    mongogo.ResetStats()
}

func (s *S) SetUpSuite(c *C) {
    mongogo.Debug(true)
    mongogo.CollectStats(true)
}

func (s *S) StartAll() {
    // Restart any stopped nodes.
    exec("cd _testdb && supervisorctl start all")
    err := exec("cd testdb && mongo --nodb wait.js")
    if err != nil {
        panic(err.String())
    }
}

func exec(command string) os.Error {
    name := "/bin/sh"
    argv := []string{name, "-c", command}
    envv := os.Environ()

    devNull, err := os.Open("/dev/null", os.O_WRONLY, 0)
    if err != nil {
        msg := fmt.Sprintf("Failed opening /dev/null: %s", err.String())
        return os.ErrorString(msg)
    }

    fdv := []*os.File{os.Stdin, devNull, os.Stderr}
    pid, err := os.ForkExec(name, argv, envv, "", fdv)
    devNull.Close()
    if err != nil {
        msg := fmt.Sprintf("Failed to execute: %s: %s", command, err.String())
        return os.ErrorString(msg)
    }
    w, err := os.Wait(pid, 0)
    if err != nil {
        return os.ErrorString(fmt.Sprintf("Error waiting for: %s: %s", command))
    }
    rc := w.ExitStatus()
    if rc != 0 {
        msg := fmt.Sprintf("%s returned non-zero exit code (%d)", command, rc)
        return os.ErrorString(msg)
    }
    return nil
}
