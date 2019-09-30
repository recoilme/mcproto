# `mcproto`

[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/recoilme/mcproto)

A simple and efficient experimental memcache protocol parser for Go.


# Getting Started

## Installing

To start using `mcproto`, install Go and run `go get`:

```sh
$ go get -u github.com/recoilme/mcproto
```

This will retrieve the library.

## Usage

```

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

// Get implementation
func (en *mapStore) Get(key []byte, rw *bufio.ReadWriter) (value []byte, noreply bool, err error) {
	en.RLock()
	defer en.RUnlock()
	value = []byte(en.m[string(key)])
	return
}

// Set implementation
func (en *mapStore) Set(key, value []byte, flags uint32, exp int32, size int, noreply bool, rw *bufio.ReadWriter) (noreplyresp bool, err error) {
	en.Lock()
	defer en.Unlock()
	en.m[string(key)] = string(value)
	return
}

// start memcache server
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
		go mcproto.ParseMc(conn, db, "")
	}
}
```

## Telnet example
```
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
```


## Contact

Vadim Kulibaba [@recoilme](https://github.com/recoilme)

## License

`mcproto` source code is available under the MIT [License](/LICENSE).