include $(GOROOT)/src/Make.inc

TARG=launchpad.net/mgo

GOFILES=\
	auth.go\
	cluster.go\
	log.go\
	queue.go\
	server.go\
	session.go\
	socket.go\
	stats.go\
	gridfs.go\

all: package

testpackage: _testdb

_testdb:
	@testdb/setup.sh start

startdb:
	@testdb/setup.sh start

stopdb:
	@testdb/setup.sh stop

clean: stopdb

GOFMT=gofmt
BADFMT:=$(shell $(GOFMT) -l $(GOFILES) $(CGOFILES) $(wildcard *_test.go) 2> /dev/null)

gofmt: $(BADFMT)
	@for F in $(BADFMT); do $(GOFMT) -w $$F && echo $$F; done

ifneq ($(BADFMT),)
ifneq ($(MAKECMDGOALS),gofmt)
$(warning WARNING: make gofmt: $(BADFMT))
endif
endif

include $(GOROOT)/src/Make.pkg

runexample: _obj/launchpad.net/mgo.a
	$(GC) -I _obj -o example.$(O) example.go
	$(LD) -L _obj -o example example.$(O)
	./example
