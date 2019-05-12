package dbtest

import (
	"bytes"
	"encoding/hex"
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
	hostPort      string // The IP address and port number of the mgo instance.
  hostname      string // The IP address or hostname of the container.
	version       string // The request MongoDB version, when running within a container
	eType         int    // Specify whether mongo should run as a container or regular process
	network       string // The name of the docker network to which the UT container should be attached
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

// SetNetwork sets the name of the docker network to which the UT container should be attached.
func (dbs *DBServer) SetNetwork(network string) {
	dbs.network = network
}

// Start Mongo DB within Docker container on host.
// It assumes Docker is already installed.
func (dbs *DBServer) execContainer(network string) *exec.Cmd {
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
	start := time.Now()
	var err error
	// Seeing intermittent issues such as:
	// Error response from daemon: Get https://registry-1.docker.io/v2/: net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)
	for time.Since(start) < 60*time.Second {
		cmd := exec.Command("docker", args...)
		if dbs.debug {
			fmt.Printf("[%s] Pulling Mongo docker image\n", time.Now().String())
			cmd.Stdout = os.Stderr
			cmd.Stderr = os.Stderr
		}
		err = cmd.Run()
		if err == nil {
			break
		} else {
			fmt.Printf("[%s] Failed to pull Mongo container image. err=%s", time.Now().String(), err.Error())
			time.Sleep(5 * time.Second)
		}
	}
	if err != nil {
		panic(err)
	}
	if dbs.debug {
		fmt.Printf("[%s] Pulled Mongo docker image\n", time.Now().String())
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

	args = []string{
		"run",
		"-t",
		"--rm", // Automatically remove the container when it exits
	}
	if network != "" {
		args = append(args, []string{
			"--net",
			network,
		}...)
	}
	args = append(args, []string{
		"--name",
		dbs.containerName,
		fmt.Sprintf("mongo:%s", dbs.version),
		"--nssize", "1",
		"--noprealloc",
		"--smallfiles",
		"--nojournal",
	}...)
	return exec.Command("docker", args...)
}

// Returns the host name of the Mongo test instance.
// If the test instance runs as a container, it returns the IP address of the container.
// If the test instance runs in the host, returns the host name.
func (dbs *DBServer) GetHostName() string {
	if dbs.eType == Docker {
		return dbs.hostname
	} else {
		if hostname, err := os.Hostname(); err != nil {
			return hostname
		} else {
			return "127.0.0.1"
		}
	}
}

// GetContainerIpAddr returns the IP address of the test Mongo instance
// The client should connect directly on the docker bridge network (such as when the client is also 
// running in a container), then client should connect to port 27017.
func (dbs *DBServer) GetContainerIpAddr() (string, error) {
	start := time.Now()
	var err error
	var stderr bytes.Buffer
	for time.Since(start) < 60*time.Second {
		stderr.Reset()
		args := []string{"inspect", "-f", "'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'", dbs.containerName}
		cmd := exec.Command("docker", args...) // #nosec
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err = cmd.Run()
		if err != nil {
			// This could be because the container has not started yet. Retry later
			fmt.Printf("[%s] Failed to get container IP address. Will retry later...\n", time.Now().String())
			time.Sleep(3 * time.Second)
			continue
		}
		ipAddr := strings.Trim(strings.TrimSpace(out.String()), "'")
		dbs.hostname = ipAddr
		return dbs.hostname, err
	}
	return "", fmt.Errorf("[%s] Failed to run command. error=%s, stderr=%s\n", time.Now().String(), err.Error(), stderr.String())
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
		dbs.hostPort = addr.String()
		dbs.server = dbs.execLocal(addr.Port)
	case Docker:
		dbs.server = dbs.execContainer(dbs.network)
	default:
		panic(fmt.Sprintf("unsupported exec type: %d", dbs.eType))
	}
	dbs.server.Stdout = &dbs.output
	dbs.server.Stderr = &dbs.output
	if dbs.debug {
		fmt.Printf("[%s] Starting Mongo instance: %v. Address: %s. Network: '%s'\n", time.Now().String(), dbs.server.Args, dbs.hostPort, dbs.network)
	}
	err = dbs.server.Start()
	if err != nil {
		panic("Failed to start Mongo instance: " + err.Error())
	}
	if dbs.debug {
		fmt.Printf("[%s] Mongo instance started\n", time.Now().String())
	}
	if dbs.eType == Docker {
		ipAddr, err2 := dbs.GetContainerIpAddr()
		if err2 != nil {
			panic(err2)
		}
		dbs.hostPort = fmt.Sprintf("%s:%d", ipAddr, 27017)
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
    fmt.Printf("[%s] Dialing mongod located at '%s'\n", time.Now().String(), dbs.hostPort)
		dbs.session, err = mgo.DialWithTimeout(dbs.hostPort+"/test", timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] Unable to dial mongod located at '%s'. Timeout=%v, Error: %s\n", time.Now().String(), dbs.hostPort, timeout, err.Error())
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
		fmt.Printf("[%s] Skip Wipe()\n", time.Now().String())
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
			fmt.Printf("Drop database '%s'\n", name)
			err = session.DB(name).DropDatabase()
			if err != nil {
				panic(err)
			}
		}
	}
}
