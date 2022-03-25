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
	"encoding/json"

	"github.com/piprate/json-gold/ld"
	"go.etcd.io/bbolt"
)

type jsonldCollection struct {
	name              string
	db                *bbolt.DB
	indexList         []Index
	refMake           ReferenceFunc
	documentProcessor *ld.JsonLdProcessor
}

func (c *jsonldCollection) NewIndex(name string, parts ...FieldIndexer) Index {
	return &index{
		name:       name,
		indexParts: parts,
		collection: c,
	}
}

func (c *jsonldCollection) Name() string {
	return c.name
}

func (c *jsonldCollection) DB() *bbolt.DB {
	return c.db
}

func (c *jsonldCollection) AddIndex(indexes ...Index) error {
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

func (c *jsonldCollection) DropIndex(name string) error {
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

func (c *jsonldCollection) Reference(doc Document) Reference {
	return c.refMake(doc)
}

// Add a json document set to the store
// this uses a single transaction per set.
func (c *jsonldCollection) Add(jsonSet []Document) error {
	return c.db.Update(func(tx *bbolt.Tx) error {
		return c.add(tx, jsonSet)
	})
}

func (c *jsonldCollection) add(tx *bbolt.Tx, jsonSet []Document) error {
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

func (c *jsonldCollection) Find(ctx context.Context, query Query) ([]Document, error) {
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

func (c *jsonldCollection) Iterate(query Query, fn DocumentWalker) error {
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
func (c *jsonldCollection) IndexIterate(query Query, fn ReferenceScanFn) error {
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
func (c *jsonldCollection) Delete(doc Document) error {
	// find matching indices and remove hash from that index
	return c.db.Update(func(tx *bbolt.Tx) error {
		return c.delete(tx, doc)
	})
}

func (c *jsonldCollection) delete(tx *bbolt.Tx, doc Document) error {
	bucket := tx.Bucket([]byte(c.name))
	if bucket == nil {
		return nil
	}

	ref := c.refMake(doc)

	docBucket := documentBucket(tx, c)
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

func (c *jsonldCollection) queryPlan(query Query) (queryPlan, error) {
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
func (c *jsonldCollection) findIndex(query Query) Index {
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

func (c *jsonldCollection) Get(key Reference) (Document, error) {
	var err error
	var data []byte

	err = c.db.View(func(tx *bbolt.Tx) error {
		bucket := documentBucket(tx, c)
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

// ValuesAtPath returns a slice with the values found at the given JSON path query
func (c *jsonldCollection) ValuesAtPath(document Document, queryPath QueryPath) ([]Scalar, error) {
	termPath, ok := queryPath.(termPath)
	if !ok {
		return nil, ErrInvalidQuery
	}

	if len(termPath.terms) == 0 {
		return []Scalar{}, nil
	}

	var input interface{}
	if err := json.Unmarshal(document, &input); err != nil {
		return nil, err
	}

	expanded, err := c.documentProcessor.Expand(input, nil)
	if err != nil {
		return nil, err
	}

	return valuesFromSliceAtPath(expanded, termPath), nil
}

func valuesFromSliceAtPath(expanded []interface{}, termPath termPath) []Scalar {
	result := make([]Scalar, 0)

	for _, sub := range expanded {
		switch typedSub := sub.(type) {
		case []interface{}:
			result = append(result, valuesFromSliceAtPath(typedSub, termPath)...)
		case map[string]interface{}:
			result = append(result, valuesFromMapAtPath(typedSub, termPath)...)
		}
	}

	return result
}

func valuesFromMapAtPath(expanded map[string]interface{}, termPath termPath) []Scalar {
	// JSON-LD in expanded form either has @value, @id, @list or @set
	if termPath.IsEmpty() {
		if value, ok := expanded["@value"]; ok {
			return []Scalar{ScalarMustParse(value)}
		}
		if id, ok := expanded["@id"]; ok {
			return []Scalar{ScalarMustParse(id)}
		}
		if list, ok := expanded["@list"]; ok {
			castList := list.([]interface{})
			scalars := make([]Scalar, len(castList))
			for i, s := range castList {
				scalars[i] = ScalarMustParse(s)
			}
			return scalars
		}
		if set, ok := expanded["@set"]; ok {
			castSet := set.([]interface{})
			scalars := make([]Scalar, len(castSet))
			for i, s := range castSet {
				scalars[i] = ScalarMustParse(s)
			}
			return scalars
		}
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
