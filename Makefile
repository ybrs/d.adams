compile:
	GOPATH=`pwd` go get github.com/syndtr/goleveldb/leveldb
	GOPATH=`pwd` go get github.com/boltdb/
	GOPATH=`pwd` go build