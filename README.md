[![Build](https://circleci.com/gh/nuts-foundation/go-leia.svg?style=svg)](https://circleci.com/gh/nuts-foundation/go-leia)
[![Coverage](https://api.codeclimate.com/v1/badges/357f0e70f6adb2793994/test_coverage)](https://codeclimate.com/github/nuts-foundation/go-leia/test_coverage  )
[![Maintainability](https://api.codeclimate.com/v1/badges/357f0e70f6adb2793994/maintainability)](https://codeclimate.com/github/nuts-foundation/go-leia/maintainability)

# go-leia

Go Lightweight Embedded Indexed (JSON) Archive

go-leia is built upon [bbolt](https://github.com/etcd-io/bbolt). 
It adds indexed based search capabilities for JSON documents to the key-value store.

The goal is to provide a simple and fast way to find relevant JSON documents using an embedded Go key-value store.
As far as performance goes, reads have preference over writes. Meaning that a search will typically be completed within 0.2ms.
A write action can take as much as 20ms depending circumstances.

## Table of Contents    

- [Installing](#installing)
- [Opening a database](#opening-a-database)
- [Collections](#collections)
    - [Writing](#writing)
    - [Reading](#reading)
    - [Searching](#searching)
- [Indexing](#indexing)
    - [Alias option](#alias-option)
    - [Transform option](#transform-option)
    - [Tokenizer option](#tokenizer-option)

## Installing

Install Go and run `go get`:

```sh
$ go get github.com/nuts-foundation/go-leia
```

When using Go > 1.16, Go modules will probably require you to install additional dependencies. 

```sh
$ go get github.com/stretchr/testify
$ go get github.com/tidwall/gjson
$ go get go.etcd.io/bbolt
```

## Opening a database

Opening a database only requires a file location for the bbolt db.

```go
package main

import (
	"log"
	
	"github.com/nuts-foundation/go-leia"
)

func main() {
	// Open the my.db data file in your current directory.
	// It will be created if it doesn't exist using filemode 0600 and default bbolt options.
	store, err := leia.NewStore("my.db")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	...
}
```

## Collections

Leia adds collections to bbolt. Each collection has its own bucket where documents are stored.
An index is also only valid for a single collection.

To create a collection:

```go
func main() {
    store, err := leia.NewStore("my.db")
	...
	
    // if a collection doesn't exist, it'll be created for you.
    // the underlying buckets are created when a document is added.
	collection := store.Collection("credentials")
}
```

### Writing

Writing a document to a collection is straightforward:

```go
func main() {
    store, err := leia.NewStore("my.db")
    collection := store.Collection("credentials")
	...
	
    // leia uses leia.Documents as arguments. Which is basically a []byte
    documents := make([]leia.Document, 1)
    documents[1] = leia.DocumentFromString("{...some json...}")
    
    // documents are added by slice
    collection.Add(documents)
}
```

Documents are added by slice. Each operation is done within a single bbolt transaction.
Make sure you don't add too many documents within a single transaction.
BBolt is a key-value store, so you've probably noticed the key is missing as an argument.
Leia computes the sha-1 of the document and uses that as key.

To get the key when needed:

```go
func main() {
    store, err := leia.NewStore("my.db")
    collection := store.Collection("credentials")
    ...
    
    // define your document
    document := leia.DocumentFromString("{...some json...}")
    
    // retrieve a leia.Reference (also a []byte)
    reference := collection.Reference(document)
}
```

Documents can also be removed:

```go
func main() {
    store, err := leia.NewStore("my.db")
    collection := store.Collection("credentials")
    ...
    
    // define your document
    document := leia.DocumentFromString("{...some json...}")
    
    // remove a document using a leia.Document
    err := collection.Delete(document)
}
```

### Reading

A document can be retrieved by reference:

```go
func main() {
    store, err := leia.NewStore("my.db")
    collection := store.Collection("credentials")
    ...
    
    // document by reference, it returns nil when not found
    document, err := collection.Get(reference)
}
```

### Searching

Reading and writing can be achieved using bbolt directly, the benefit of leia is searching.
The performance of a search greatly depends on the available indices on a collection.
If no index matches the query, a bbolt cursor is used to loop over all documents in the collection.

Leia supports equal, prefix and range queries. 
The first argument for each matcher is the JSON path using the syntax from [gjson](github.com/tidwall/gjson).
Only basic path syntax is used. There is no support for wildcards or comparison operators.
The second argument is the value to match against.
Leia can only combine query terms using **AND** logic.

```go
func main() {
    ...
    
    // define a new query
    query := leia.New(leia.Eq("subject", "some_value")).
                  And(leia.Range("some.path.#.amount", 1, 100))
}
```

Getting results can be done with either `Find` or `Iterate`. 
`Find` will return a slice of documents. `Iterate` will allow you to pass a `DocWalker` which is called for each hit.

```go
func main() {
    ...
    
    // get a slice of documents
    documents, err := collection.Find(query)
    
    // use a DocWalker
    walker := func(ref []byte, doc []byte) error {
    	// do something with the document
    }
    err := collection.Iterate(query, walker)
}
```

## Indexing

Indexing JSON documents is where the real added value of leia lies.
For each collection multiple indices can be added.

An index can be added and removed:

```go
func main() {
    ...
    
    // define the index
    index := leia.NewIndex("compound",
                leia.NewFieldIndexer("subject"),
                leia.NewFieldIndexer("some.path.#.amount"),
    )
    
    // add it to the collection
    err := collection.AddIndex(index)
    
    // remove it from the collection
    err := collection.DropIndex("compound")
}
```

The argument for `NewFieldIndexer` uses the same notation as the query parameter, also without wildcards or comparison operators.
Adding an index will trigger a re-index of all documents in the collection.
Adding an index with a duplicate name will ignore the index.

### Alias option

Leia support indexing JSON paths under an **alias**.
An alias can be used to index different documents but use a single query to find both.

```go
func main() {
    ...
    
    // define the index for credentialX
    indexX := leia.NewIndex("credentialX", leia.NewFieldIndexer("credentialSubject.id", leia.AliasOption{Alias: "subject"}))
    // define the index for credentialY
    indexY := leia.NewIndex("credentialY", leia.NewFieldIndexer("credentialSubject.organization.id", leia.AliasOption{Alias: "subject"}))
    
    ...

    // define a new query
    query := leia.New(leia.Eq("subject", "some_value"))
}
```

The example above defines two indices to a collection, each index has a different JSON path to be indexed.
Both indices will be used when the given query is executed, resulting in documents that match either index.

### Transform option

A transformer can be defined for a `FieldIndexer`. A transformer will transform the indexed value and query parameter.
This can be used to allow case-insensitive search or add a soundex style index.

```go
func main() {
    ...
    
    // This index transforms all values to lowercase
    index := leia.NewIndex("credential", leia.NewFieldIndexer("subject", leia.TransformOption{Transform: leia.ToLower}))
    
    ...

    // these queries will be the same
    query1 := leia.New(leia.Eq("subject", "VALUE"))
    query2 := leia.New(leia.Eq("subject", "value"))
}
```

### Tokenizer option

Sometimes JSON fields contain a whole text. 
Leia has a tokenizer option to split a value at a JSON path into multiple keys to be indexed.
For example, the sentence `"The quick brown fox jumps over the lazy dog"` could be tokenized so the document can easily be found when the term `fox` is used in a query.
A more advanced tokenizer could also remove common words like `the`.

```go
func main() {
    ...
    
    // This index transforms all values to lowercase
    index := leia.NewIndex("credential", leia.NewFieldIndexer("text", leia.TokenizerOption{Tokenizer: leia.WhiteSpaceTokenizer}))
    
    ...

    // will match {"text": "The quick brown fox jumps over the lazy dog"}
    query := leia.New(leia.Eq("subject", "fox"))
}
```

All options can be combined.
