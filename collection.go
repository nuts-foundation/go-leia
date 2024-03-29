/*
 * go-leia
 * Copyright (C) 2022 Nuts community
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package leia

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/piprate/json-gold/ld"
	"github.com/tidwall/gjson"
	"go.etcd.io/bbolt"
)

// ErrNoIndex is returned when no index is found to query against
var ErrNoIndex = errors.New("no index found")

// DocumentWalker defines a function that is used as a callback for matching documents.
// The key will be the document Reference (hash) and the value will be the raw document bytes
type DocumentWalker func(key Reference, value []byte) error

// documentCollection is the bucket that stores all the documents for a collection
const documentCollection = "_documents"

func documentCollectionByteRef() []byte {
	return []byte(documentCollection)
}

// Collection defines a logical collection of documents and indices within a store.
type Collection interface {
	// AddIndex to this collection. It doesn't matter if the index already exists.
	// If you want to override an index (by path) drop it first.
	AddIndex(index ...Index) error
	// DropIndex by path
	DropIndex(name string) error
	// NewIndex creates a new index from the context of this collection
	// If multiple field indexers are given, a compound index is created.
	NewIndex(name string, parts ...FieldIndexer) Index
	// Add a set of documents to this collection
	Add(jsonSet []Document) error
	// Get returns the data for the given key or nil if not found
	Get(ref Reference) (Document, error)
	// Delete a document
	Delete(doc Document) error
	// Find queries the collection for documents
	// returns ErrNoIndex when no suitable index can be found
	// returns context errors when the context has been cancelled or deadline has exceeded.
	// passing ctx prevents adding too many records to the result set.
	Find(ctx context.Context, query Query) ([]Document, error)
	// Reference uses the configured reference function to generate a reference of the function
	Reference(doc Document) Reference
	// Iterate over documents that match the given query
	Iterate(query Query, walker DocumentWalker) error
	// IndexIterate is used for iterating over indexed values. The query keys must match exactly with all the FieldIndexer.Name() of an index
	// returns ErrNoIndex when no suitable index can be found
	IndexIterate(query Query, fn ReferenceScanFn) error
	// ValuesAtPath returns a slice with the values found by the configured valueCollector
	ValuesAtPath(document Document, queryPath QueryPath) ([]Scalar, error)
	// DocumentCount returns the number of indexed documents
	DocumentCount() (int, error)
}

// ReferenceFunc is the func type used for creating references.
// references are the key under which a document is stored.
// a ReferenceFunc could be the sha256 func or something that stores document in chronological order.
// The first would be best for random access, the latter for chronological access
type ReferenceFunc func(doc Document) Reference

// default for shasum docs
func defaultReferenceCreator(doc Document) Reference {
	s := sha1.Sum(doc)
	var b = make([]byte, len(s))
	copy(b, s[:])

	return b
}

type collection struct {
	name           string
	db             *bbolt.DB
	indexList      []Index
	refMake        ReferenceFunc
	documentLoader ld.DocumentLoader
	collectionType CollectionType
	valueCollector valueCollector
}

func (c *collection) NewIndex(name string, parts ...FieldIndexer) Index {
	return &index{
		name:       name,
		indexParts: parts,
		collection: c,
	}
}

func (c *collection) AddIndex(indexes ...Index) error {
	for _, index := range indexes {
		for _, i := range c.indexList {
			if i.Name() == index.Name() {
				return nil
			}
		}

		if err := c.db.Update(func(tx *bbolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte(c.name))
			if err != nil {
				return err
			}

			// skip existing
			if b := bucket.Bucket(index.BucketName()); b != nil {
				return nil
			}

			gBucket, err := bucket.CreateBucketIfNotExists(documentCollectionByteRef())
			if err != nil {
				return err
			}

			cur := gBucket.Cursor()
			for ref, doc := cur.First(); ref != nil; ref, doc = cur.Next() {
				index.Add(bucket, ref, doc)
			}

			return nil
		}); err != nil {
			return err
		}

		c.indexList = append(c.indexList, index)
	}

	return nil
}

func (c *collection) DropIndex(name string) error {
	return c.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(c.name))
		if err != nil {
			return err
		}

		var newIndices = make([]Index, len(c.indexList))
		j := 0
		for _, i := range c.indexList {
			if name == i.Name() {
				bucket.DeleteBucket(i.BucketName())
			} else {
				newIndices[j] = i
				j++
			}
		}
		c.indexList = newIndices[:j]
		return nil
	})
}

func (c *collection) Reference(doc Document) Reference {
	return c.refMake(doc)
}

// Add a json document set to the store
// this uses a single transaction per set.
func (c *collection) Add(jsonSet []Document) error {
	return c.db.Update(func(tx *bbolt.Tx) error {
		return c.add(tx, jsonSet)
	})
}

func (c *collection) add(tx *bbolt.Tx, jsonSet []Document) error {
	bucket, err := tx.CreateBucketIfNotExists([]byte(c.name))
	if err != nil {
		return err
	}

	docBucket, err := bucket.CreateBucketIfNotExists(documentCollectionByteRef())
	if err != nil {
		return err
	}

	for _, doc := range jsonSet {
		ref := c.refMake(doc)

		// indices
		// buckets are cached within tx
		for _, i := range c.indexList {
			err = i.Add(bucket, ref, doc)
			if err != nil {
				return err
			}
		}

		err = docBucket.Put(ref, doc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *collection) Find(ctx context.Context, query Query) ([]Document, error) {
	docs := make([]Document, 0)
	walker := func(key Reference, value []byte) error {
		// stop iteration when needed
		if err := ctx.Err(); err != nil {
			return err
		}

		docs = append(docs, value)
		return nil
	}

	if err := c.Iterate(query, walker); err != nil {
		return nil, err
	}

	return docs, nil
}

func (c *collection) Iterate(query Query, fn DocumentWalker) error {
	plan, err := c.queryPlan(query)
	if err != nil {
		return err
	}
	if err = plan.execute(fn); err != nil {
		return err
	}

	return nil
}

// IndexIterate uses a query to loop over all keys and Entries in an index. It skips the resultScan and collect phase
func (c *collection) IndexIterate(query Query, fn ReferenceScanFn) error {
	index := c.findIndex(query)
	if index == nil {
		return ErrNoIndex
	}

	plan := indexScanQueryPlan{
		queryPlanBase: queryPlanBase{
			collection: c,
			query:      query,
		},
		index: index,
	}

	return plan.execute(fn)
}

// Delete a document from the store, this also removes the entries from indices
func (c *collection) Delete(doc Document) error {
	// find matching indices and remove hash from that index
	return c.db.Update(func(tx *bbolt.Tx) error {
		return c.delete(tx, doc)
	})
}

func (c *collection) delete(tx *bbolt.Tx, doc Document) error {
	bucket := tx.Bucket([]byte(c.name))
	if bucket == nil {
		return nil
	}

	ref := c.refMake(doc)

	docBucket := c.documentBucket(tx)
	if docBucket == nil {
		return nil
	}
	err := docBucket.Delete(ref)
	if err != nil {
		return err
	}

	// indices
	for _, i := range c.indexList {
		err = i.Delete(bucket, ref, doc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *collection) queryPlan(query Query) (queryPlan, error) {
	index := c.findIndex(query)

	if index == nil {
		return fullTableScanQueryPlan{
			queryPlanBase: queryPlanBase{
				collection: c,
				query:      query,
			},
		}, nil
	}

	return resultScanQueryPlan{
		queryPlanBase: queryPlanBase{
			collection: c,
			query:      query,
		},
		index: index,
	}, nil
}

// find a matching index.
// The index may, at most, be one longer than the number of search options.
// The longest index will win.
func (c *collection) findIndex(query Query) Index {
	// first map the indices to the number of matching search options
	var cIndex Index
	var cMatch float64

	for _, i := range c.indexList {
		m := i.IsMatch(query)
		if m > cMatch {
			cIndex = i
			cMatch = m
		}
	}

	return cIndex
}

func (c *collection) Get(key Reference) (Document, error) {
	var err error
	var data []byte

	err = c.db.View(func(tx *bbolt.Tx) error {
		bucket := c.documentBucket(tx)
		if bucket == nil {
			return nil
		}

		data = bucket.Get(key)
		return nil
	})

	if data == nil {
		return nil, nil
	}

	return data, err
}

func (c *collection) DocumentCount() (int, error) {
	var count int
	err := c.db.View(func(tx *bbolt.Tx) error {
		bucket := c.documentBucket(tx)
		if bucket == nil {
			return nil
		}

		count = bucket.Stats().KeyN
		return nil
	})
	return count, err
}

func (c *collection) documentBucket(tx *bbolt.Tx) *bbolt.Bucket {
	bucket := tx.Bucket([]byte(c.name))
	if bucket == nil {
		return nil
	}
	return bucket.Bucket(documentCollectionByteRef())
}

// valueCollector is responsible for going through the document and finding the Scalars that match the Query
type valueCollector func(collection *collection, document Document, queryPath QueryPath) ([]Scalar, error)

// JSONPathValueCollector collects values at a given JSON path expression. Objects are delimited by a dot and lists use an extra # in the expression:
// object.list.#.key
func JSONPathValueCollector(_ *collection, document Document, queryPath QueryPath) ([]Scalar, error) {
	jsonPath, ok := queryPath.(jsonPath)
	if !ok {
		return nil, ErrInvalidQuery
	}

	if !gjson.ValidBytes(document) {
		return nil, ErrInvalidJSON
	}
	result := gjson.GetBytes(document, string(jsonPath))

	return valuesFromResult(result)
}

// JSONLDValueCollector collects values given a list of IRIs that represent the nesting of the objects.
func JSONLDValueCollector(collection *collection, document Document, queryPath QueryPath) ([]Scalar, error) {
	iriPath, ok := queryPath.(iriPath)
	if !ok {
		return nil, ErrInvalidQuery
	}

	var input interface{}
	if err := json.Unmarshal(document, &input); err != nil {
		return nil, err
	}

	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = collection.documentLoader
	expanded, err := ld.NewJsonLdProcessor().Expand(input, options)
	if err != nil {
		return nil, err
	}

	return valuesFromSliceAtPath(expanded, iriPath), nil
}

func valuesFromSliceAtPath(expanded []interface{}, termPath iriPath) []Scalar {
	result := make([]Scalar, 0)

	for _, sub := range expanded {
		switch typedSub := sub.(type) {
		case []interface{}:
			result = append(result, valuesFromSliceAtPath(typedSub, termPath)...)
		case map[string]interface{}:
			result = append(result, valuesFromMapAtPath(typedSub, termPath)...)
		case string:
			result = append(result, MustParseScalar(typedSub))
		case bool:
			result = append(result, MustParseScalar(typedSub))
		case float64:
			result = append(result, MustParseScalar(typedSub))
		}
	}

	return result
}

func valuesFromMapAtPath(expanded map[string]interface{}, termPath iriPath) []Scalar {
	// JSON-LD in expanded form either has @value, @id, @list or @set
	if termPath.IsEmpty() {
		if value, ok := expanded["@value"]; ok {
			return []Scalar{MustParseScalar(value)}
		}
		if id, ok := expanded["@id"]; ok {
			return []Scalar{MustParseScalar(id)}
		}
		if list, ok := expanded["@list"]; ok {
			castList := list.([]interface{})
			return valuesFromSliceAtPath(castList, termPath)
		}
	}

	if list, ok := expanded["@list"]; ok {
		castList := list.([]interface{})
		return valuesFromSliceAtPath(castList, termPath)
	}

	if value, ok := expanded[termPath.Head()]; ok {
		// the value should now be a slice
		next, ok := value.([]interface{})
		if !ok {
			return nil
		}
		return valuesFromSliceAtPath(next, termPath.Tail())
	}

	return nil
}

// ValuesAtPath returns a slice with the values found at the given JSON path query
func (c *collection) ValuesAtPath(document Document, queryPath QueryPath) ([]Scalar, error) {
	return c.valueCollector(c, document, queryPath)
}

func valuesFromResult(result gjson.Result) ([]Scalar, error) {
	switch result.Type {
	case gjson.String:
		return []Scalar{StringScalar(result.Str)}, nil
	case gjson.True:
		return []Scalar{BoolScalar(true)}, nil
	case gjson.False:
		return []Scalar{BoolScalar(false)}, nil
	case gjson.Number:
		return []Scalar{Float64Scalar(result.Num)}, nil
	case gjson.Null:
		return []Scalar{}, nil
	default:
		if result.IsArray() {
			keys := make([]Scalar, 0)
			for _, subResult := range result.Array() {
				subKeys, err := valuesFromResult(subResult)
				if err != nil {
					return nil, err
				}
				keys = append(keys, subKeys...)
			}
			return keys, nil
		}
	}
	return nil, fmt.Errorf("type at path not supported for indexing: %s", result.String())
}
