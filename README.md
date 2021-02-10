[![Build](https://circleci.com/gh/nuts-foundation/go-leia.svg?style=svg)](https://circleci.com/gh/nuts-foundation/go-leia)
[![Coverage](https://codecov.io/gh/nuts-foundation/go-leia/branch/master/graph/badge.svg)](https://codecov.io/gh/nuts-foundation/go-leia)
[![Maintainability](https://api.codeclimate.com/v1/badges/357f0e70f6adb2793994/maintainability)](https://codeclimate.com/github/nuts-foundation/go-leia/maintainability)

# go-leia

Go Lightweigt Embedded Indexed (JSON) Archive

built upon bbolt

## todo

- indices must use sub-buckets otherwise index names will overlap
- time type (rfc3337 will work?)
- custom key generation for better distribution per collection  
- "or" logic?
- lowmem test

perf

1m entries >> 700mb file >> 0.2 ms search time
deletes and writes take longer... >> 50ms
The same when using compound indices of 4 lvls

## mocks

```
mockgen -destination=db_mock.go -package=leia -source=db.go
mockgen -destination=index_mock.go -package=leia -source=index.go
mockgen -destination=collection_mock.go -package=leia -source=collection.go
```