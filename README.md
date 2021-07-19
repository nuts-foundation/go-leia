[![Build](https://circleci.com/gh/nuts-foundation/go-leia.svg?style=svg)](https://circleci.com/gh/nuts-foundation/go-leia)
[![Coverage](https://api.codeclimate.com/v1/badges/357f0e70f6adb2793994/test_coverage)](https://codeclimate.com/github/nuts-foundation/go-leia/test_coverage  )
[![Maintainability](https://api.codeclimate.com/v1/badges/357f0e70f6adb2793994/maintainability)](https://codeclimate.com/github/nuts-foundation/go-leia/maintainability)

# go-leia

Go Lightweigt Embedded Indexed (JSON) Archive

built upon bbolt

## todo

- time type (rfc3337 will work?)
- custom key generation for better distribution per collection  
- "or" logic?
- lowmem test

perf

1m entries >> 700mb file >> 0.2 ms search time
deletes and writes take longer... >> 50ms
The same when using compound indices of 4 lvls
