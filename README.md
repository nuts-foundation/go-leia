[![nuts-foundation](https://circleci.com/gh/nuts-foundation/nuts-node.svg?style=svg)](https://circleci.com/gh/nuts-foundation/nuts-node)


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