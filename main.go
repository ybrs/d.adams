package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	//"os"
	"github.com/syndtr/goleveldb/leveldb"
	"strconv"
	"strings"
	"sync"
	"github.com/syndtr/goleveldb/leveldb/opt"
	//"time"
	"os"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/boltdb/bolt"
)


type store struct {
	data map[string]string
	lock *sync.RWMutex
}

func (store *store) Get(key string) string {
	store.lock.RLock()
	defer store.lock.RUnlock()

	return store.data[key]
}

func (store *store) Set(key string, val string) {
	store.lock.Lock()
	defer store.lock.Unlock()

	store.data[key] = val
}


type Counter struct {
	cnt int64
	sync.RWMutex
}


type client struct {
	id     int64
	conn   net.Conn
	reader *bufio.Reader
	store  *store
	counter *Counter
	clientid string
	db *leveldb.DB
	bdb *bolt.DB
	offset uint64
}



func (client *client) nextNum() (int64){
	// this is a global counter
	client.counter.Lock()
	defer client.counter.Unlock()
	client.counter.cnt += 1
	client.db.Put([]byte("internalcounter_1"), []byte(strconv.FormatInt(client.counter.cnt, 10)), nil)
	return client.counter.cnt
}

func (client *client) UpdateOffset(offset uint64) {
    // check if client id is in the bucket
    err := client.bdb.Update(func(tx *bolt.Tx) error {
	bucket, err := tx.CreateBucketIfNotExists([]byte("CLIENT_OFFSETS"))
	if err != nil {
		log.Fatal(err)
	}
	val := bucket.Get([]byte(client.clientid))
	if val == nil {
		// first time
		fmt.Println("setting client offset - first time", client.clientid, 0)

		err = bucket.Put([]byte(client.clientid), []byte(string(0)))
		if err != nil {
			log.Fatal(err)
		}
		return nil
	}

	fmt.Println("setting client offset - ", client.clientid, offset)
	err = bucket.Put([]byte(client.clientid), []byte(string(offset)))
	if err != nil {
		log.Fatal(err)
	}
	return nil
    })

	if err != nil {
		log.Fatal(err)
	}

}

func (client *client) GetOffset(){
    err := client.bdb.Update(func(tx *bolt.Tx) error {
	bucket, err := tx.CreateBucketIfNotExists([]byte("CLIENT_OFFSETS"))
	if err != nil {
		log.Fatal(err)
	}
	val := bucket.Get([]byte(client.clientid))
	if val == nil {
		// first time
		fmt.Println("setting client offset - first time", client.clientid, 0)

		err = bucket.Put([]byte(client.clientid), []byte("0"))
		if err != nil {
			log.Fatal(err)
		}
		return nil
	}

	    c, err := strconv.ParseUint(string(val), 10, 64)
	    client.offset = c

	    if err != nil {
		    log.Fatal(err)
	    }

	return nil
    })

	if err != nil {
		log.Fatal(err)
	}
}

func (client *client) serve() {

	fmt.Println("x 1 0 0")

	// defer client.conn.Close()

	 client.log("Accepted connection: %s", client.conn.LocalAddr())
	client.reader = bufio.NewReader(client.conn)

	for {
		fmt.Println("x 1 0")
		cmd, err := client.readCommand()
		if err != nil {
			if err == io.EOF {
				client.log("Disconnected")
			} else if _, ok := err.(protocolError); ok {
				client.sendError(err)
			} else {
				client.logError("readCommand(): %s", err)
			}
			return
		}

		fmt.Println("xxxx --- 0")

		switch strings.ToUpper(cmd.Name) {
		case "PUBLISH":
			fmt.Println("xxxx --- 1")
			// push channel args....
			if len(cmd.Args) < 2 {
				client.sendError(fmt.Errorf("PUSH expects 2 arguments"))
			} else {
				// strconv.FormatInt(time.Now().UnixNano(), 10)
				c := client.nextNum()
				n := []string{cmd.Args[0], strconv.FormatInt(c, 10)}
				k := strings.Join(n, "_")
				client.db.Put([]byte(k), []byte(cmd.Args[1]), nil)
				client.send(k)
			}

		case "BLPOP":
			if len(cmd.Args) < 1 {
				client.sendError(fmt.Errorf("POP expects 1 argument"))
			}
			//client.send("hello", "bar")
			for _, arg := range(cmd.Args){
				fmt.Println("args", arg)
			}

		case "CLIENTID":
			client.clientid = cmd.Args[0]
			client.sendOk()

		case "ACK":

			c, err := strconv.ParseUint(cmd.Args[1], 10, 64)
			if err != nil {
				client.sendError(err)
				break
			}
			client.UpdateOffset(c)
			client.sendOk()

		case "SUBSCRIBE":
			// this just sends X num of items in the channel
			// then subscribes to channel X starting from 0
			// or where the client has left off
			fmt.Println("-> args ->", cmd.Args)
			client.GetOffset()
			startNum := client.offset
			iter := client.db.NewIterator(&util.Range{
					Start: []byte(strings.Join([]string{cmd.Args[0], strconv.FormatUint(startNum, 10)}, "_")),
			}, nil)
			defer iter.Release()
			cnt := 0
			for iter.Next() {
				key := iter.Key()
				value := iter.Value()
				fmt.Println("sending - ", string(key), string(value))
				client.send("message", cmd.Args[0], string(key) + "||" + string(value))
				cnt += 1
				if cnt > 10 {
					break
				}
			}


		case "POP":
			if len(cmd.Args) < 1 {
				client.sendError(fmt.Errorf("POP expects 1 argument"))
			}
			client.send("hello", "bar")

		// case "GET":
		// 	if len(cmd.Args) < 1 {
		// 		client.sendError(fmt.Errorf("GET expects 1 argument"))
		// 		return
		// 	}
		// 	val := client.store.Get(cmd.Args[0])
		// 	client.send(val)
		// case "SET":
		// 	if len(cmd.Args) < 1 {
		// 		client.sendError(fmt.Errorf("SET expects 2 arguments"))
		// 		return
		// 	}
		// 	client.store.Set(cmd.Args[0], cmd.Args[1])
		// 	fmt.Fprintf(client.conn, "+OK\r\n")
		default:
			client.sendError(fmt.Errorf("unknown command: %s", cmd.Name))
		}
	}
}

func (client *client) log(msg string, args ...interface{}) {
	prefix := fmt.Sprintf("Client #%d: ", client.id)
	log.Printf(prefix + msg, args...)
}

func (client *client) logError(msg string, args ...interface{}) {
	client.log("Error: "+msg, args...)
}

func (client *client) sendOk(){
	fmt.Fprintf(client.conn, "+OK\r\n")
}

func (client *client) send(vals ...string) {
	// fmt.Fprintf(client.conn, "$%d\r\n%s\r\n", len(val), val)
	fmt.Fprintf(client.conn, "*%d\r\n", len(vals))
	for _, val := range(vals){
		fmt.Fprintf(client.conn, "$%d\r\n%s\r\n", len(val), val)
	}
}

func (client *client) sendError(err error) {
	client.logError(err.Error())
	client.sendLine("-ERR " + err.Error() + "\r\n")
}

func (client *client) sendLine(line string) {
	if _, err := io.WriteString(client.conn, line); err != nil {
		client.log("Error for client.sendLine(): %s", err)
	}
}

type protocolError string

func (e protocolError) Error() string {
	return string(e)
}

func (client *client) readCommand() (*command, error) {
	for {
		line, err := client.readLine()

		if err != nil {
			return nil, err
		}

		// Example: *5 (command consisting of 5 arguments)
		if !strings.HasPrefix(line, "*") {
			return &command{Name: line}, nil
		}

		argcStr := line[1:]
		argc, err := strconv.ParseUint(argcStr, 10, 64)
		if err != nil || argc <= 1 {
			return nil, protocolError("invalid argument count: " + argcStr)
		}

		args := make([]string, 0, argc)
		for i := 0; i < int(argc); i++ {
			line, err := client.readLine()
			if err != nil {
				return nil, err
			}

			// Example: $3 (next line has 3 bytes + \r\n)
			if !strings.HasPrefix(line, "$") {
				return nil, protocolError("unknown command: " + line)
			}

			argLenStr := line[1:]
			argLen, err := strconv.ParseUint(argLenStr, 10, 64)
			if err != nil {
				return nil, protocolError("invalid argument length: " + argLenStr)
			}

			arg := make([]byte, argLen+2)
			if _, err := io.ReadFull(client.reader, arg); err != nil {
				return nil, err
			}

			args = append(args, string(arg[0:len(arg)-2]))
		}

		return &command{Name: args[0], Args: args[1:]}, nil
	}
}

func (client *client) readLine() (string, error) {
	var line string
	for {
		partialLine, isPrefix, err := client.reader.ReadLine()
		if err != nil {
			return "", err
		}

		line += string(partialLine)
		if isPrefix {
			continue
		}

		return line, nil
	}
}

type command struct {
	Name string
	Args []string
}



type numberComparer struct{}

func (numberComparer) num(x []byte) (n int) {
	fmt.Sscan(string(x[1:len(x)-1]), &n)
	return
}

func (numberComparer) Name() string {
	return "test.NumberComparer"
}

func (p numberComparer) Compare(a, b []byte) int {
	s1 := string(a)
	s2 := string(b)

	r1 := strings.Split(s1, "_")
	g1 := r1[0]
	v1 := r1[1]

	r2 := strings.Split(s2, "_")
	g2 := r2[0]
	v2 := r2[1]

	if g1 != g2 {
		return strings.Compare(string(a), string(b))
	}

	c1, _ := strconv.ParseInt(v1, 10, 64)
	c2, _ := strconv.ParseInt(v2, 10, 64)
	return int(c1 - c2)
}


func (numberComparer) Separator(dst, a, b []byte) []byte {
	return nil
}
func (numberComparer) Successor(dst, b []byte) []byte    {
	return nil
}


func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile)


	db, err := leveldb.OpenFile("./db", &opt.Options{
		DisableLargeBatchTransaction: true,
		Comparer:                     numberComparer{},
	})
	if err != nil {
		fmt.Println("couldnt open db.", err)
		return
	}

	bdb, err := bolt.Open("./db/boltdb.bdb", 0644, nil)
	if err != nil {
        	log.Fatal(err)
    	}
	defer bdb.Close()


	//err = db.Put([]byte("foo_3"), []byte("value"), nil)
	//err = db.Put([]byte("foo_2"), []byte("value"), nil)
	//err = db.Put([]byte("foo_1"), []byte("value"), nil)
	//err = db.Put([]byte("bar_1"), []byte("value"), nil)


	//err = db.Put([]byte("foo_2"), []byte("value"), nil)
	//err = db.Put([]byte("foo_3"), []byte("value"), nil)
	//err = db.Put([]byte("foo_1"), []byte("value"), nil)
	//err = db.Put([]byte("bar_1"), []byte("value"), nil)

	//if err != nil {
	//	fmt.Println("err", err)
	//}
	//
	//t1 := time.Now()
	//
	//for i := 0; i < 100000; i++ {
	//	err = db.Put([]byte(fmt.Sprintf("foo_%d", i)), []byte("value"), nil)
	//}
	//
	//fmt.Println("elapsed", time.Now().Sub(t1))


	//iter := db.NewIterator(nil, nil)
	//for iter.Next() {
	//    	key := iter.Key()
	//    	value := iter.Value()
	//	fmt.Println("------------->", string(key), string(value))
	//}

	cnt := &Counter{}

	fmt.Println(cnt)

	cnt.Lock()
	v, err := db.Get([]byte("internalcounter_1"), nil)
	if err != nil {
		v = []byte(strconv.FormatInt(0, 10))
		db.Put([]byte("internalcounter_1"), []byte(strconv.FormatInt(0, 10)), nil)
	}

	cnt.cnt, err = strconv.ParseInt(string(v), 10, 64)
	cnt.Unlock()
	defer db.Close()



	log.Printf("Server started\n")
	addr := ":8080"
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Printf("Error: listen(): %s", err)
		os.Exit(1)
	}

	log.Printf("Accepting connections at: %s", addr)
	store := &store{
		data: make(map[string]string),
		lock: &sync.RWMutex{},
	}

	var id int64
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error: Accept(): %s", err)
			continue
		}

		id++
		client := &client{id: id, conn: conn, store: store, db: db, counter: cnt, bdb: bdb}
		go client.serve()
	}
}

