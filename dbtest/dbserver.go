package dbtest

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
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
	exposePort    bool   // Specify whether container port should be exposed to the host OS.
	debug         bool   // Log debug statements
	containerName string // The container name, when running mgo within a container
	tomb          tomb.Tomb
	rsName        string // ReplicaSet Name. If not empty- the mongod will be started as a Replica Set Server
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

// SetExposePort sets whether the container port should be exposed to the host OS.
func (dbs *DBServer) SetExposePort(exposePort bool) {
	dbs.exposePort = exposePort
}

// SetReplicaSetName sets whether the mongod is started as a replica set
func (dbs *DBServer) SetReplicaSetName(rsName string) {
	dbs.rsName = rsName
}

// SetContainerName sets the name of the docker container when the DB instance is started within a container.
func (dbs *DBServer) SetContainerName(containerName string) {
	dbs.containerName = containerName
}

func (dbs *DBServer) pullDockerImage(dockerImage string) {
	// Check if the docker image exists in the local registry.
	args := []string{
		"images",
		"-q",
		dockerImage,
	}
	cmd := exec.Command("docker", args...)
	err := cmd.Run()
	if err == nil {
		// The image is already present locally.
		// Do not invoke docker pull because:
		// 1. Every network operations counts towards the dockerhub API rate limiting.
		// 2. Reduce the chance of intermittent network issues.
		return
	}

	// It may take a long time to download the mongo image if the docker image is not installed.
	// Execute 'docker pull' now to pull the image before executing it. Otherwise Dial() may fail
	// with a timeout after 10 seconds.
	args = []string{
		"pull",
		dockerImage,
	}
	start := time.Now()
	var stdout, stderr bytes.Buffer
	// Seeing intermittent issues such as:
	// Error response from daemon: Get https://registry-1.docker.io/v2/: net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)
	for time.Since(start) < 60*time.Second {
		cmd := exec.Command("docker", args...)
		log.Printf("Pulling Mongo docker image %s", dockerImage)
		if dbs.debug {
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
		}
		err = cmd.Run()
		if err == nil {
			break
		} else {
			log.Printf("Failed to pull Mongo container image. err=%s\n%s\n%s",
				err.Error(), stdout.String(), stderr.String())
			time.Sleep(5 * time.Second)
		}
	}
	if err != nil {
		panic(err)
	}
	log.Printf("Pulled Mongo docker image")
}

// Start Mongo DB within Docker container on host.
// It assumes Docker is already installed.
func (dbs *DBServer) execContainer(network string, exposePort bool) *exec.Cmd {
	if dbs.version == "" {
		dbs.version = "latest"
	}

	dockerImage := fmt.Sprintf("mongo:%s", dbs.version)
	dbs.pullDockerImage(dockerImage)

	args := []string{
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
	if exposePort {
		args = append(args, []string{"-p", fmt.Sprintf("%d:%d", 27017, 27017)}...)
	}
	args = append(args, []string{
		"--name",
		dbs.containerName,
		fmt.Sprintf("mongo:%s", dbs.version),
	}...)

	if dbs.rsName != "" {
		args = append(args, []string{
			"mongod",
			"--replSet",
			dbs.rsName,
		}...)
	}
	log.Printf("DB start up arguments are: %v", args)
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

// GetContainerName returns the name of the container, when running the Mongo UT instance in a container.
func (dbs *DBServer) GetContainerName() string {
	return dbs.containerName
}

// GetContainerIpAddr returns the IP address of the test Mongo instance
// The client should connect directly on the docker bridge network (such as when the client is also
// running in a container), then client should connect to port 27017.
func (dbs *DBServer) GetContainerIpAddr() (string, error) {
	start := time.Now()
	var err error
	var stderr bytes.Buffer
	for time.Since(start) < 60*time.Second {
		if dbs.server.ProcessState != nil {
			// The process has exited
			log.Printf("Mongo container has exited unexpectedly. Output:\n%s", dbs.output.String())
			return "", fmt.Errorf("Process has exited")
		}
		stderr.Reset()
		args := []string{"inspect", "-f", "'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'", dbs.containerName}
		cmd := exec.Command("docker", args...) // #nosec
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err = cmd.Run()
		if err != nil {
			// This could be because the container has not started yet. Retry later
			log.Printf("Failed to get container IP address. Will retry later. Err: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		ipAddr := strings.Trim(strings.TrimSpace(out.String()), "'")
		dbs.hostname = ipAddr
		log.Printf("Mongo IP address is %v", dbs.hostname)
		if dbs.network == "" {
			return "127.0.0.1", nil
		} else {
			return dbs.hostname, err
		}
	}
	log.Printf("Unable to get container IP address: %v", err)
	return "", fmt.Errorf("Failed to run command. error=%s, stderr=%s\n", err.Error(), stderr.String())
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
	}

	if dbs.rsName != "" {
		args = append(args, []string{
			"--replSet",
			dbs.rsName,
		}...)
	}
	return exec.Command("mongod", args...)
}

func (dbs *DBServer) start() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)
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

	if dbs.containerName == "" {
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
	}

	dbs.tomb = tomb.Tomb{}
	switch dbs.eType {
	case LocalProcess:
		dbs.hostPort = addr.String()
		dbs.server = dbs.execLocal(addr.Port)
	case Docker:
		dbs.server = dbs.execContainer(dbs.network, dbs.exposePort)
	default:
		panic(fmt.Sprintf("unsupported exec type: %d", dbs.eType))
	}
	dbs.server.Stdout = &dbs.output
	dbs.server.Stderr = &dbs.output
	log.Printf("Starting Mongo instance: %v. Address: %s. Network: '%s'", dbs.server.Args, dbs.hostPort, dbs.network)
	err = dbs.server.Start()
	if err != nil {
		panic("Failed to start Mongo instance: " + err.Error())
	}
	log.Printf("Mongo instance started")
	go func() {
		// Call Wait() so cmd.ProcessState is set after command has completed.
		err = dbs.server.Wait()
		if err != nil {
			log.Printf("Command exited. Output:\n%s", dbs.output.String())
		}
	}()
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
		log.Printf("Dialing mongod located at '%s'. timeout: %v", dbs.hostPort, timeout)
		// If The MongoDB Driver is Configured with ReplicaSet - (Then configure Replica Set first!)
		// Directly Connect First - and then configure Replica Set!
		if dbs.rsName != "" {
			dbs.session, err = mgo.DialWithTimeout(dbs.hostPort+"/test?connect=direct", timeout)
			if err != nil {
				log.Printf("Unable to dial mongod located at '%s'. Timeout=%v, Error: %s", dbs.hostPort, timeout, err.Error())
				log.Printf("%s", dbs.output.Bytes())
				dbs.printMongoDebugInfo()
				panic(err)
			}
			err = dbs.Initiate()
			if err != nil {
				log.Printf("Unable to Configure Replica Set '%s'. Timeout=%v, Error: %s", dbs.rsName, timeout, err.Error())
				log.Printf("%s", dbs.output.Bytes())
				dbs.printMongoDebugInfo()
				panic(err)
			}
			dbs.session.Close() // Create a new One Below without Direct=true...

		}
		dbs.session, err = mgo.DialWithTimeout(dbs.hostPort+"/test", timeout)
		if err != nil {
			log.Printf("Unable to dial mongod located at '%s'. Timeout=%v, Error: %s", dbs.hostPort, timeout, err.Error())
			log.Printf("%s", dbs.output.Bytes())
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
		log.Printf("Skip Wipe()")
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
			log.Printf("Drop database '%s'", name)
			err = session.DB(name).DropDatabase()
			if err != nil {
				panic(err)
			}
		}
	}
}

// Code From https://github.com/juju/replicaset To Start MongoDB in replica Set Configuration
// Config is the document stored in mongodb that defines the servers in the
// replica set
type Config struct {
	Name            string   `bson:"_id"`
	Version         int      `bson:"version"`
	ProtocolVersion int      `bson:"protocolVersion,omitempty"`
	Members         []Member `bson:"members"`
}

// Member holds configuration information for a replica set member.
//
// See http://docs.mongodb.org/manual/reference/replica-configuration/
// for more details
type Member struct {
	// Id is a unique id for a member in a set.
	Id int `bson:"_id"`

	// Address holds the network address of the member,
	// in the form hostname:port.
	Address string `bson:"host"`
}

func (dbs *DBServer) Initiate() error {
	monotonicSession := dbs.session.Clone()
	defer monotonicSession.Close()
	monotonicSession.SetMode(mgo.Monotonic, true)
	protocolVersion := 1
	var err error
	// We don't know mongod's ability to use a correct IPv6 addr format
	// until the server is started, but we need to know before we can start
	// it. Try the older, incorrect format, if the correct format fails.
	cfg := []Config{
		{
			Name:            dbs.rsName,
			Version:         1,
			ProtocolVersion: protocolVersion,
			Members: []Member{{
				Id:      0,
				Address: dbs.hostPort,
			}},
		},
	}

	// Attempt replSetInitiate, with potential retries.
	for i := 0; i < 5; i++ {
		monotonicSession.Refresh()
		if err = doAttemptInitiate(monotonicSession, cfg); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}

	// Wait for replSetInitiate to complete. Even if err != nil,
	// it may be that replSetInitiate is still in progress, so
	// attempt CurrentStatus.
	for i := 0; i < 10; i++ {
		monotonicSession.Refresh()
		var status *Status
		status, err = getCurrentStatus(monotonicSession)
		if err != nil {
			log.Printf("Initiate: fetching replication status failed: %v", err)
		}
		if err != nil || len(status.Members) == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		break
	}
	return err
}

// CurrentStatus returns the status of the replica set for the given session.
func getCurrentStatus(session *mgo.Session) (*Status, error) {
	status := &Status{}
	err := session.Run("replSetGetStatus", status)
	if err != nil {
		return nil, fmt.Errorf("cannot get replica set status: %v", err)
	}

	for index, member := range status.Members {
		status.Members[index].Address = member.Address
	}
	return status, nil
}

// Status holds data about the status of members of the replica set returned
// from replSetGetStatus
//
// See http://docs.mongodb.org/manual/reference/command/replSetGetStatus/#dbcmd.replSetGetStatus
type Status struct {
	Name    string         `bson:"set"`
	Members []MemberStatus `bson:"members"`
}

// Status holds the status of a replica set member returned from
// replSetGetStatus.
type MemberStatus struct {
	// Id holds the replica set id of the member that the status is describing.
	Id int `bson:"_id"`

	// Address holds address of the member that the status is describing.
	Address string `bson:"name"`

	// Self holds whether this is the status for the member that
	// the session is connected to.
	Self bool `bson:"self"`

	// ErrMsg holds the most recent error or status message received
	// from the member.
	ErrMsg string `bson:"errmsg"`

	// Healthy reports whether the member is up. It is true for the
	// member that the request was made to.
	Healthy bool `bson:"health"`

	// State describes the current state of the member.
	State MemberState `bson:"state"`
}

// doAttemptInitiate will attempt to initiate a mongodb replicaset with each of
// the given configs, returning as soon as one config is successful.
func doAttemptInitiate(monotonicSession *mgo.Session, cfg []Config) error {
	var err error
	for _, c := range cfg {
		if err = monotonicSession.Run(bson.D{{"replSetInitiate", c}}, nil); err != nil {
			log.Printf("Unsuccessful attempt to initiate replicaset: %v", err)
			continue
		}
		return nil
	}
	return err
}

type MemberState int

const (
	StartupState = iota
	PrimaryState
	SecondaryState
	RecoveringState
	FatalState
	Startup2State
	UnknownState
	ArbiterState
	DownState
	RollbackState
	ShunnedState
)

var memberStateStrings = []string{
	StartupState:    "STARTUP",
	PrimaryState:    "PRIMARY",
	SecondaryState:  "SECONDARY",
	RecoveringState: "RECOVERING",
	FatalState:      "FATAL",
	Startup2State:   "STARTUP2",
	UnknownState:    "UNKNOWN",
	ArbiterState:    "ARBITER",
	DownState:       "DOWN",
	RollbackState:   "ROLLBACK",
	ShunnedState:    "SHUNNED",
}

// String returns a string describing the state.
func (state MemberState) String() string {
	if state < 0 || int(state) >= len(memberStateStrings) {
		return "INVALID_MEMBER_STATE"
	}
	return memberStateStrings[state]
}
