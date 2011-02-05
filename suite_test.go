package mongogo_test

import (
    .   "gocheck"
    "testing"
    "mongogo"
    "time"
    "fmt"
    "os"
    "strings"
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
    stopped bool
}

var _ = Suite(&S{})

func (s *S) SetUpSuite(c *C) {
    mongogo.Debug(true)
    mongogo.CollectStats(true)
}

func (s *S) SetUpTest(c *C) {
    err := exec("mongo --nodb testdb/dropall.js")
    if err != nil {
        panic(err.String())
    }
    mongogo.SetLogger((*cLogger)(c))
    mongogo.ResetStats()
}

func (s *S) TearDownTest(c *C) {
    if s.stopped {
        s.StartAll()
    }
}

func (s *S) Stop(host string) {
    err := exec("cd _testdb && supervisorctl stop " + supvName(host))
    if err != nil {
        panic(err.String())
    }
    s.stopped = true
}

func (s *S) StartAll() {
    // Restart any stopped nodes.
    exec("cd _testdb && supervisorctl start all")
    err := exec("cd testdb && mongo --nodb wait.js")
    if err != nil {
        panic(err.String())
    }
    s.stopped = false
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

// supvName returns the supervisord name for the given host address.
func supvName(host string) string {
    switch {
    case strings.HasSuffix(host, ":40001"):
        return "db1"
    case strings.HasSuffix(host, ":40011"):
        return "rs1a"
    case strings.HasSuffix(host, ":40012"):
        return "rs1b"
    case strings.HasSuffix(host, ":40013"):
        return "rs1c"
    case strings.HasSuffix(host, ":40021"):
        return "rs2a"
    case strings.HasSuffix(host, ":40022"):
        return "rs2b"
    case strings.HasSuffix(host, ":40023"):
        return "rs2c"
    case strings.HasSuffix(host, ":40101"):
        return "cfg1"
    case strings.HasSuffix(host, ":40201"):
        return "s1"
    case strings.HasSuffix(host, ":40202"):
        return "s2"
    }
    panic("Unknown host: " + host)
}
