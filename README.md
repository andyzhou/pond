# About
This is a big file storage library

# How to use?
Please see `example` sub dir.

# Testing
go test -bench="Read" -benchtime=5s .

`
goos: darwin
goarch: amd64
pkg: github.com/andyzhou/pond/testing
cpu: Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz
BenchmarkRead-8           259346             22170 ns/op
PASS`

