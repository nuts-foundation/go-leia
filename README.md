[![nuts-foundation](https://circleci.com/gh/nuts-foundation/go-leia.svg?style=svg)](https://circleci.com/gh/nuts-foundation/go-leia)

[![nuts-foundation](https://codecov.io/gh/nuts-foundation/go-leia/branch/master/graph/badge.svg)](https://codecov.io/gh/nuts-foundation/go-leia)

# go-leia

Go Lightweigt Embedded Indexed (JSON) Archive

built upon bbolt

## todo

- cleanup (types, interfaces)
- testing
- performance test

later

- compound index
- range logic
- time type (rfc3337 will work?)
- and/or logic

perf

100k entries >> 100mb file >> 0.1 ms search time
deletes and writes take longer... >> 50ms