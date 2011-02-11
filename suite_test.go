package mongogo_test

import (
    "exec"
    .   "gocheck"
    "testing"
    "mongogo"
    "time"
    "fmt"
    "io/ioutil"
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
    s.StartAll()
}

func (s *S) SetUpTest(c *C) {
    err := run("mongo --nodb testdb/dropall.js")
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
    for i := 0; ; i++ {
        stats := mongogo.GetStats()
        if stats.SocketsInUse == 0 && stats.SocketsAlive == 0 {
            break
        }
        if i == 20 {
            c.Fatal("Test left sockets in a dirty state")
        }
        c.Logf("Waiting for sockets to die: %d in use, %d alive", stats.SocketsInUse, stats.SocketsAlive)
        time.Sleep(5e8)
    }
}

func (s *S) Stop(host string) {
    err := run("cd _testdb && supervisorctl stop " + supvName(host))
    if err != nil {
        panic(err.String())
    }
    s.stopped = true
}

func (s *S) StartAll() {
    // Restart any stopped nodes.
    run("cd _testdb && supervisorctl start all")
    err := run("cd testdb && mongo --nodb wait.js")
    if err != nil {
        panic(err.String())
    }
    s.stopped = false
}

func run(command string) os.Error {
    name := "/bin/sh"
    argv := []string{name, "-c", command}
    envv := os.Environ()

    p, err := exec.Run(name, argv, envv, "", exec.PassThrough, exec.Pipe, exec.PassThrough)
    if err != nil {
        msg := fmt.Sprintf("Failed to execute: %s: %s", command, err.String())
        return os.ErrorString(msg)
    }
    output, _ := ioutil.ReadAll(p.Stdout)
    p.Stdout.Close()
    w, err := p.Wait(0)
    if err != nil {
        return os.ErrorString(fmt.Sprintf("Error waiting for: %s: %s", command))
    }
    rc := w.ExitStatus()
    if rc != 0 {
        msg := fmt.Sprintf("%s returned non-zero exit code (%d):\n%s", command, rc, output)
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
