# `mcproto`

[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/recoilme/mcproto)

A simple and efficient memcache protocol parser for Go.


# Getting Started

## Installing

To start using `mcproto`, install Go and run `go get`:

```sh
$ go get -u github.com/recoilme/mcproto
```

This will retrieve the library.

## Usage

See test

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