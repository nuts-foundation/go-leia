/*
 * go-leia
 * Copyright (C) 2021 Nuts community
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
	"fmt"

	"github.com/tidwall/gjson"
	"go.etcd.io/bbolt"
)

type jsonCollection struct {
	name      string
	db        *bbolt.DB
	indexList []Index
	refMake   ReferenceFunc
}

func (c *jsonCollection) NewIndex(name string, parts ...FieldIndexer) Index {
	return &index{
		name:       name,
		indexParts: parts,
		collection: c,
	}
}

func (c *jsonCollection) AddIndex(indexes ...Index) error {
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

func (c *jsonCollection) DropIndex(name string) error {
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

func (c *jsonCollection) Reference(doc Document) Reference {
	return c.refMake(doc)
}

// Add a json document set to the store
// this uses a single transaction per set.
func (c *jsonCollection) Add(jsonSet []Document) error {
	return c.db.Update(func(tx *bbolt.Tx) error {
		return c.add(tx, jsonSet)
	})
}

func (c *jsonCollection) add(tx *bbolt.Tx, jsonSet []Document) error {
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

func (c *jsonCollection) Find(ctx context.Context, query Query) ([]Document, error) {
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

func (c *jsonCollection) Iterate(query Query, fn DocumentWalker) error {
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
func (c *jsonCollection) IndexIterate(query Query, fn ReferenceScanFn) error {
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
func (c *jsonCollection) Delete(doc Document) error {
	// find matching indices and remove hash from that index
	return c.db.Update(func(tx *bbolt.Tx) error {
		return c.delete(tx, doc)
	})
}

func (c *jsonCollection) delete(tx *bbolt.Tx, doc Document) error {
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

func (c *jsonCollection) queryPlan(query Query) (queryPlan, error) {
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
func (c *jsonCollection) findIndex(query Query) Index {
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

func (c *jsonCollection) Get(key Reference) (Document, error) {
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

func (c *jsonCollection) documentBucket(tx *bbolt.Tx) *bbolt.Bucket {
	bucket := tx.Bucket([]byte(c.name))
	if bucket == nil {
		return nil
	}
	return bucket.Bucket(documentCollectionByteRef())
}

// ValuesAtPath returns a slice with the values found at the given JSON path query
func (c *jsonCollection) ValuesAtPath(document Document, queryPath QueryPath) ([]Scalar, error) {
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

func valuesFromResult(result gjson.Result) ([]Scalar, error) {
	switch result.Type {
	case gjson.String:
		return []Scalar{{value: result.Str}}, nil
	case gjson.Number:
		return []Scalar{{value: result.Num}}, nil
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
