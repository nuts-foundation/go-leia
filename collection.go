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
	"github.com/nuts-foundation/go-stoabs"
	"github.com/piprate/json-gold/ld"
	"github.com/tidwall/gjson"
	"go.etcd.io/bbolt"
)

// ErrNoIndex is returned when no index is found to query against
var ErrNoIndex = errors.New("no index found")

// DocumentWalker defines a function that is used as a callback for matching documents.
// The key will be the document stoabs.Key (hash) and the value will be the raw document bytes
type DocumentWalker func(key stoabs.Key, value []byte) error

// documentCollection is the bucket that stores all the documents for a collection
const documentCollection = "_documents"

type bucket string

func (b bucket) shelf() string {
	return string(b)
}

func (b bucket) child(name string) bucket {
	return bucket(string(b) + "." + name)
}

func (b bucket) childBytes(name []byte) bucket {
	return bucket(string(b) + "." + string(name))
}

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
	Get(ref stoabs.Key) (Document, error)
	// Delete a document
	Delete(doc Document) error
	// Find queries the collection for documents
	// returns ErrNoIndex when no suitable index can be found
	// returns context errors when the context has been cancelled or deadline has exceeded.
	// passing ctx prevents adding too many records to the result set.
	Find(ctx context.Context, query Query) ([]Document, error)
	// Reference uses the configured reference function to generate a reference of the function
	Reference(doc Document) stoabs.Key
	// Iterate over documents that match the given query
	Iterate(query Query, walker DocumentWalker) error
	// IndexIterate is used for iterating over indexed values. The query keys must match exactly with all the FieldIndexer.Name() of an index
	// returns ErrNoIndex when no suitable index can be found
	IndexIterate(query Query, fn ReferenceScanFn) error
	// ValuesAtPath returns a slice with the values found by the configured valueCollector
	ValuesAtPath(document Document, queryPath QueryPath) ([]Scalar, error)
}

// ReferenceFunc is the func type used for creating references.
// references are the key under which a document is stored.
// a ReferenceFunc could be the sha256 func or something that stores document in chronological order.
// The first would be best for random access, the latter for chronological access
type ReferenceFunc func(doc Document) stoabs.Key

// default for shasum docs
func defaultReferenceCreator(doc Document) stoabs.Key {
	s := sha1.Sum(doc)
	var b = make([]byte, len(s))
	copy(b, s[:])

	return stoabs.BytesKey(b)
}

type collection struct {
	name           string
	db             stoabs.KVStore
	indexList      []Index
	refMake        ReferenceFunc
	documentLoader ld.DocumentLoader
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

		indexBucket := bucket(c.name).childBytes(index.BucketName())
		documentBucket := bucket(c.name).child(documentCollection)

		err := c.db.Write(context.Background(), func(tx stoabs.WriteTx) error {
			// If index exists, skip
			var exists bool
			err := tx.GetShelfReader(indexBucket.shelf()).
				Iterate(func(key stoabs.Key, value []byte) error {
					exists = true
					return errors.New("stop")
				}, stoabs.BytesKey{})
			if err.Error() != "stop" || exists {
				return err
			}

			err = tx.GetShelfReader(documentBucket.shelf()).Iterate(func(ref stoabs.Key, doc []byte) error {
				return index.Add(bucket(c.name), ref, doc)
			}, stoabs.BytesKey{})

			return err
		})
		if err != nil {
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

func (c *collection) Reference(doc Document) stoabs.Key {
	return c.refMake(doc)
}

// Add a json document set to the store
// this uses a single transaction per set.
func (c *collection) Add(jsonSet []Document) error {
	return c.db.Write(context.Background(), func(tx stoabs.WriteTx) error {
		return c.add(tx, jsonSet)
	})
}

func (c *collection) add(tx stoabs.WriteTx, jsonSet []Document) error {
	//bucket, err := tx.CreateBucketIfNotExists([]byte(c.name))
	//if err != nil {
	//	return err
	//}
	//
	//docBucket, err := bucket.CreateBucketIfNotExists(documentCollectionByteRef())
	//if err != nil {
	//	return err
	//}

	for _, doc := range jsonSet {
		ref := c.refMake(doc)

		// indices
		// buckets are cached within tx
		for _, i := range c.indexList {
			err := i.Add(tx, bucket(c.name), ref, doc)
			if err != nil {
				return err
			}
		}

		writer, err := tx.GetShelfWriter(bucket(c.name).child(documentCollection).shelf())
		if err != nil {
			return err
		}
		err = writer.Put(ref, doc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *collection) Find(ctx context.Context, query Query) ([]Document, error) {
	docs := make([]Document, 0)
	walker := func(key stoabs.Key, value []byte) error {
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
	return c.db.Write(context.Background(), func(tx stoabs.WriteTx) error {
		return c.delete(tx, doc)
	})
}

func (c *collection) delete(tx stoabs.WriteTx, doc Document) error {
	bucket := bucket(c.name)
	ref := c.refMake(doc)

	// Remove document
	docWriter, err := tx.GetShelfWriter(bucket.child(documentCollection).shelf())
	if err != nil {
		return err
	}
	err = docWriter.Delete(ref)
	if err != nil {
		return err
	}

	// Remove indices
	for _, i := range c.indexList {
		err = i.Delete(tx, bucket, ref, doc)
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

func (c *collection) Get(key stoabs.Key) (Document, error) {
	var err error
	var data []byte

	err = c.db.ReadShelf(context.Background(), bucket(c.name).child(documentCollection).shelf(), func(reader stoabs.Reader) error {
		data, err = reader.Get(key)
		return err
	})

	if data == nil {
		return nil, nil
	}

	return data, err
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
