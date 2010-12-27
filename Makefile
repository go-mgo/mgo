include $(GOROOT)/src/Make.inc

TARG=mongogo
GOFMT=gofmt -spaces=true -tabindent=false -tabwidth=4

GOFILES=\
	session.go\
	cluster.go\
	server.go\
	socket.go\
	stats.go\
	cond.go\
	log.go\

include $(GOROOT)/src/Make.pkg

format:
	${GOFMT} -w session.go
	${GOFMT} -w cluster.go
	${GOFMT} -w server.go
	${GOFMT} -w socket.go
	${GOFMT} -w stats.go
	${GOFMT} -w log.go
	${GOFMT} -w mongogo_test.go

testpackage: _testdb

_testdb:
	@testdb/setup.sh start

startdb:
	@testdb/setup.sh start

stopdb:
	@testdb/setup.sh stop

clean: stopdb
