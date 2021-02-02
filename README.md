[![Build](https://circleci.com/gh/nuts-foundation/go-leia.svg?style=svg)](https://circleci.com/gh/nuts-foundation/go-leia)
[![Coverage](https://codecov.io/gh/nuts-foundation/go-leia/branch/master/graph/badge.svg)](https://codecov.io/gh/nuts-foundation/go-leia)
[![Maintainability](https://api.codeclimate.com/v1/badges/357f0e70f6adb2793994/maintainability)](https://codeclimate.com/github/nuts-foundation/go-leia/maintainability)

# go-leia

Go Lightweigt Embedded Indexed (JSON) Archive

built upon bbolt

## todo

- compound index
- range logic
- time type (rfc3337 will work?)
- and/or logic

perf

100k entries >> 100mb file >> 0.1 ms search time
deletes and writes take longer... >> 50ms