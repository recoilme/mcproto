package mcproto_test

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/recoilme/mcproto"
)

type mapStore struct {
	sync.RWMutex
	m map[string]string
}

func newStore() mcproto.McEngine {
	eng := &mapStore{}
	eng.Lock()
	defer eng.Unlock()
	eng.m = make(map[string]string)
	return eng
}

// implementation
func (en *mapStore) Get(key []byte, rw *bufio.ReadWriter) (value []byte, noreply bool, err error) {
	en.RLock()
	defer en.RUnlock()
	value = []byte(en.m[string(key)])
	return
}

func (en *mapStore) Gets(keys [][]byte, rw *bufio.ReadWriter) (err error) {
	return
}

func (en *mapStore) Set(key, value []byte, flags uint32, exp int32, size int, noreply bool, rw *bufio.ReadWriter) (noreplyresp bool, err error) {
	en.Lock()
	defer en.Unlock()
	en.m[string(key)] = string(value)
	return
}

func (en *mapStore) Incr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error) {
	return
}

func (en *mapStore) Decr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error) {
	return
}

func (en *mapStore) Delete(key []byte, rw *bufio.ReadWriter) (isFound bool, noreply bool, err error) {
	return
}

func (en *mapStore) Close() (err error) {
	return
}

func Test_Store(t *testing.T) {
	db := newStore()
	db.Set([]byte("1"), []byte("2"), 0, 0, 1, false, nil)
	val, _, _ := db.Get([]byte("1"), nil)
	if string(val) != "2" {
		t.Errorf("Expected 2, got:%s", val)
	}
	valmis, _, _ := db.Get([]byte("mis"), nil)
	if string(valmis) != "" {
		t.Errorf("Expected '', got:%s", valmis)
	}
}

/*
telnet 127.0.0.1 11212
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
set hello
ERROR
set key 0 0 5
value
STORED
get key
VALUE key 0 5
value
END
*/
func Test_Listen(t *testing.T) {
	db := newStore()

	listener, err := net.Listen("tcp", ":11212")
	if err != nil {
		t.Error(err)
	}
	defer listener.Close()

	// start
	for {

		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("conn", err)
			conn.Close()
			continue
		}
		go mcproto.ParseMc(conn, db, "") //listen(conn, db)
	}
}

//func listen(c net.Conn, db mcproto.McEngine) {
//mcproto.ParseMc(c, db, "")
//}
