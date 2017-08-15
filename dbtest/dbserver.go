package dbtest

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/tomb.v2"
)

// Constants to define how the DB test instance should be executed
const (
	// Run MongoDB as local process
	LocalProcess = 0
	// Run MongoDB within a docker container
	Docker = 1
)

// DBServer controls a MongoDB server process to be used within test suites.
//
// The test server is started when Session is called the first time and should
// remain running for the duration of all tests, with the Wipe method being
// called between tests (before each of them) to clear stored data. After all tests
// are done, the Stop method should be called to stop the test server.
//
// Before the DBServer is used the SetPath method must be called to define
// the location for the database files to be stored.
type DBServer struct {
	session       *mgo.Session
	output        bytes.Buffer
	server        *exec.Cmd
	dbpath        string
	host          string
	version       string // The request MongoDB version, when running within a container
	eType         int    // Specify whether mongo should run as a container or regular process
	debug         bool   // Log debug statements
	containerName string // The container name, when running mgo within a container
	tomb          tomb.Tomb
}

// SetPath defines the path to the directory where the database files will be
// stored if it is started. The directory path itself is not created or removed
// by the test helper.
func (dbs *DBServer) SetPath(dbpath string) {
	dbs.dbpath = dbpath
}

// SetVersion defines the desired MongoDB version to run within a container.
// The attribute is ignored when running MongoDB outside a container.
func (dbs *DBServer) SetVersion(version string) {
	dbs.version = version
}

func (dbs *DBServer) SetDebug(enableDebug bool) {
	dbs.debug = enableDebug
}

// SetExecType specifies if the DB instance should run locally or as a container.
func (dbs *DBServer) SetExecType(execType int) {
	dbs.eType = execType
}

// Start Mongo DB within Docker container on host.
// It assumes Docker is already installed
func (dbs *DBServer) execContainer(port int) *exec.Cmd {
	if dbs.version == "" {
		dbs.version = "latest"
	}
	// It may take a long time to download the mongo image if the docker image is not installed.
	// Execute 'docker pull' now to pull the image before executing it. Otherwise Dial() may fail
	// with a timeout after 10 seconds.
	args := []string{
		"pull",
		fmt.Sprintf("mongo:%s", dbs.version),
	}
	cmd := exec.Command("docker", args...)
	if dbs.debug {
		fmt.Printf("Pulling Mongo docker image\n")
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	}
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	if dbs.debug {
		fmt.Printf("Pulled Mongo docker image\n")
	}

	// Generate a name for the container. This will help to inspect the container
	// and get the Mongo PID.
	u := make([]byte, 8)
	// The default number generator is deterministic.
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	_, err = r.Read(u)
	if err != nil {
		panic(err)
	}
	dbs.containerName = fmt.Sprintf("mongo-%s", hex.EncodeToString(u))

	// On some platforms, we get "chown: changing ownership of '/proc/1/fd/1': Permission denied" unless
	// we allocate a pseudo tty (-t option)
	var portArg string
	if port > 0 {
		portArg = fmt.Sprintf("%d:%d", port, 27017)
	} else {
		// Let docker allocate the port.
		// This is useful when multiple tests are running on the same host
		portArg = fmt.Sprintf("%d", 27017)
	}
	args = []string{
		"run",
		"-t",
		"-p",
		portArg,
		"--rm", // Automatically remove the container when it exits
		"--name",
		dbs.containerName,
		fmt.Sprintf("mongo:%s", dbs.version),
		"--nssize", "1",
		"--noprealloc",
		"--smallfiles",
		"--nojournal",
	}
	return exec.Command("docker", args...)
}

// Returns the host name of the Mongo test instance.
// If the test instance runs as a container, it returns the container name.
// If the test instance runs in the host, returns the host name.
func (dbs *DBServer) GetHostName() string {
	if dbs.eType == Docker {
		return dbs.containerName
	} else {
		if hostname, err := os.Hostname(); err != nil {
			return hostname
		} else {
			return "127.0.0.1"
		}
	}
}

// GetContainerHostPort returns the Host port for the test Mongo instance
func (dbs *DBServer) GetContainerHostPort() (int, error) {
	start := time.Now()
	var err error
	var stderr bytes.Buffer
	for time.Since(start) < 30*time.Second {
		stderr.Reset()
		args := []string{"port", dbs.containerName, fmt.Sprintf("%d", 27017)}
		cmd := exec.Command("docker", args...) // #nosec
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err = cmd.Run()
		if err != nil {
			// This could be because the container has not started yet. Retry later
			fmt.Printf("Failed to get container host port number. Will retry later...\n")
			time.Sleep(3 * time.Second)
			continue
		}
		s := strings.TrimSpace(out.String())
		o := strings.Split(s, ":")
		if len(o) < 2 {
			fmt.Printf("Unable to get container host port number: %s", s)
			return -1, errors.New(fmt.Sprintf("Unable to get container host port number. %s", s))
		}
		i, err2 := strconv.Atoi(o[1])
		if err2 != nil {
			fmt.Printf("Unable to parse port number: error=%s, out=%s\n", err2.Error(), o[1])
		}
		return i, err2
	}
	fmt.Printf("Failed to run command. error=%s, stderr=%s\n", err.Error(), stderr.String())
	return -1, err
}

// Stop the docker container running Mongo.
func (dbs *DBServer) stopContainer() {
	args := []string{
		"stop",
		dbs.containerName,
	}
	cmd := exec.Command("docker", args...)
	if dbs.debug {
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	}
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	// Remove the container and its unamed volume.
	// In some cases the "docker run --rm" option does not remove the container.
	args = []string{
		"rm",
		"-v",
		dbs.containerName,
	}
	cmd = exec.Command("docker", args...)
	if dbs.debug {
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
	}
	err = cmd.Run()
}

// Start Mongo DB as process on host. It assumes Mongo is already installed
func (dbs *DBServer) execLocal(port int) *exec.Cmd {
	args := []string{
		"--dbpath", dbs.dbpath,
		"--bind_ip", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--nssize", "1",
		"--noprealloc",
		"--smallfiles",
		"--nojournal",
	}
	return exec.Command("mongod", args...)
}

func (dbs *DBServer) start() {
	if dbs.server != nil {
		panic("DBServer already started")
	}
	if dbs.dbpath == "" {
		panic("DBServer.SetPath must be called before using the server")
	}
	mgo.SetStats(true)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic("unable to listen on a local address: " + err.Error())
	}
	addr := l.Addr().(*net.TCPAddr)
	l.Close()

	dbs.tomb = tomb.Tomb{}
	switch dbs.eType {
	case LocalProcess:
		dbs.host = addr.String()
		dbs.server = dbs.execLocal(addr.Port)
	case Docker:
		dbs.server = dbs.execContainer(0)
	default:
		panic(fmt.Sprintf("unsupported exec type: %d", dbs.eType))
	}
	dbs.server.Stdout = &dbs.output
	dbs.server.Stderr = &dbs.output
	if dbs.debug {
		fmt.Printf("[%s] Starting Mongo instance: %v. Address: %s\n", time.Now().String(), dbs.server.Args, dbs.host)
	}
	err = dbs.server.Start()
	if err != nil {
		panic("Failed to start Mongo instance: " + err.Error())
	}
	if dbs.debug {
		fmt.Printf("[%s] Mongo instance started\n", time.Now().String())
	}
	if dbs.eType == Docker {
		p, err2 := dbs.GetContainerHostPort()
		if err2 != nil {
			panic(err2)
		}
		dbs.host = fmt.Sprintf("127.0.0.1:%d", p)
	}
	dbs.tomb.Go(dbs.monitor)
	dbs.Wipe()
}

func (dbs DBServer) printMongoDebugInfo() {
	fmt.Fprintf(os.Stderr, "[%s] mongod processes running right now:\n", time.Now().String())
	cmd := exec.Command("/bin/sh", "-c", "ps auxw | grep mongod")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.Run()
	if dbs.eType == Docker {
		fmt.Fprintf(os.Stderr, "[%s] mongod containers running right now:\n", time.Now().String())
		cmd := exec.Command("/bin/sh", "-c", "docker ps -a |grep mongo")
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		cmd.Run()

		args := []string{
			"inspect",
			dbs.containerName,
		}
		cmd = exec.Command("docker", args...)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		fmt.Fprintf(os.Stderr, "[%s] Container inspect:\n", time.Now().String())
		cmd.Run()
		args = []string{
			"logs",
			dbs.containerName,
		}
		cmd = exec.Command("docker", args...)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		fmt.Fprintf(os.Stderr, "[%s] Container logs:\n", time.Now().String())
		cmd.Run()
	}
	fmt.Fprintf(os.Stderr, "----------------------------------------\n")
}

func (dbs *DBServer) monitor() error {
	dbs.server.Process.Wait()
	if dbs.tomb.Alive() {
		// Present some debugging information.
		fmt.Fprintf(os.Stderr, "---- mongod process died unexpectedly:\n")
		fmt.Fprintf(os.Stderr, "%s", dbs.output.Bytes())
		dbs.printMongoDebugInfo()

		panic("mongod process died unexpectedly")
	}
	return nil
}

// Stop stops the test server process, if it is running.
//
// It's okay to call Stop multiple times. After the test server is
// stopped it cannot be restarted.
//
// All database sessions must be closed before or while the Stop method
// is running. Otherwise Stop will panic after a timeout informing that
// there is a session leak.
func (dbs *DBServer) Stop() {
	if dbs.session != nil {
		dbs.checkSessions()
		if dbs.session != nil {
			dbs.session.Close()
			dbs.session = nil
		}
	}
	if dbs.server != nil {
		dbs.tomb.Kill(nil)
		if dbs.eType == Docker {
			// Invoke 'docker stop'
			dbs.stopContainer()
		}
		dbs.server.Process.Signal(os.Interrupt)
		select {
		case <-dbs.tomb.Dead():
		case <-time.After(5 * time.Second):
			panic("timeout waiting for mongod process to die")
		}
		dbs.server = nil
	}
}

// Session returns a new session to the server. The returned session
// must be closed after the test is done with it.
//
// The first Session obtained from a DBServer will start it.
func (dbs *DBServer) SessionWithTimeout(timeout time.Duration) *mgo.Session {
	if dbs.server == nil {
		dbs.start()
	}
	if dbs.session == nil {
		mgo.ResetStats()
		var err error
		dbs.session, err = mgo.DialWithTimeout(dbs.host+"/test", timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] Unable to dial mongod. Timeout=%v, Error: %s\n", time.Now().String(), timeout, err.Error())
			fmt.Fprintf(os.Stderr, "%s", dbs.output.Bytes())
			dbs.printMongoDebugInfo()
			panic(err)
		}
	}
	return dbs.session.Copy()
}

// Session returns a new session to the server. The returned session
// must be closed after the test is done with it.
//
// The first Session obtained from a DBServer will start it.
func (dbs *DBServer) Session() *mgo.Session {
	return dbs.SessionWithTimeout(10 * time.Second)
}

// checkSessions ensures all mgo sessions opened were properly closed.
// For slightly faster tests, it may be disabled setting the
// environmnet variable CHECK_SESSIONS to 0.
func (dbs *DBServer) checkSessions() {
	if check := os.Getenv("CHECK_SESSIONS"); check == "0" || dbs.server == nil || dbs.session == nil {
		return
	}
	dbs.session.Close()
	dbs.session = nil
	for i := 0; i < 100; i++ {
		stats := mgo.GetStats()
		if stats.SocketsInUse == 0 && stats.SocketsAlive == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	stats := mgo.GetStats()
	panic(fmt.Sprintf("There are mgo sessions still alive. SocketCount=%d InUseCount=%d",
		stats.SocketsAlive, stats.SocketsInUse))
}

// Wipe drops all created databases and their data.
//
// The MongoDB server remains running if it was prevoiusly running,
// or stopped if it was previously stopped.
//
// All database sessions must be closed before or while the Wipe method
// is running. Otherwise Wipe will panic after a timeout informing that
// there is a session leak.
func (dbs *DBServer) Wipe() {
	if dbs.server == nil || dbs.session == nil {
		return
	}
	dbs.checkSessions()
	sessionUnset := dbs.session == nil
	session := dbs.Session()
	defer session.Close()
	if sessionUnset {
		dbs.session.Close()
		dbs.session = nil
	}
	names, err := session.DatabaseNames()
	if err != nil {
		panic(err)
	}
	for _, name := range names {
		switch name {
		case "admin", "local", "config":
		default:
			err = session.DB(name).DropDatabase()
			if err != nil {
				panic(err)
			}
		}
	}
}
