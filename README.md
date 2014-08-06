# mgo: MongoDB driver for Go

## Running the Tests:
1) [Install Go](http://golang.org/doc/install)

2) [Install Supervisor](http://supervisord.org/installing.html)

3) `mkdir -p ~/mgo-gopath/src/gopkg.in && git clone git@github.com:go-mgo/mgo.git ~/mgo-gopath/src/gopkg.in/mgo.v2 && export GOPATH=~/mgo-gopath`

4) Install dependencies:
`go get gopkg.in/mgo.v2/bson`
`go get gopkg.in/check.v1`

5) Run tests:
```
make startdb
go test -gocheck.v
make stopdb
```
