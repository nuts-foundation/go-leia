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
	"crypto/sha1"
	"errors"

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
	// If you want to override an index (by name) drop it first.
	AddIndex(index Index) error
	// DropIndex by name
	DropIndex(name string) error
	// Add a set of documents to this collection
	Add(jsonSet []Document) error
	// Get returns the data for the given key or nil if not found
	Get(ref Reference) (*Document, error)
	// Delete a document
	Delete(doc Document) error
	// Find queries the collection for documents
	// returns ErrNoIndex when no suitable index can be found
	Find(query Query) ([]Document, error)
	// Reference uses the configured reference function to generate a reference of the function
	Reference(doc Document) Reference
	// Iterate over documents that match the given query
	Iterate(query Query, walker DocumentWalker) error
	// IndexIterate is used for iterating over indexed values. The query keys must match exactly with all the FieldIndexer.Name() of an index
	// returns ErrNoIndex when no suitable index can be found
	IndexIterate(query Query, fn ReferenceScanFn) error
}

// ReferenceFunc is the func type used for creating references.
// references are the key under which a document is stored.
// a ReferenceFunc could be the sha256 func or something that stores document in chronological order.
// The first would be best for random access, the latter for chronological access
type ReferenceFunc func(doc Document) Reference

// default for shasum docs
func defaultReferenceCreator(doc Document) Reference {
	s := sha1.Sum(doc.raw)
	var b = make([]byte, len(s))
	copy(b, s[:])

	return b
}

type collection struct {
	Name      string `json:"name"`
	db        *bbolt.DB
	IndexList []Index `json:"indices"`
	refMake   ReferenceFunc
}

func (c *collection) AddIndex(index Index) error {
	for _, i := range c.IndexList {
		if i.Name() == index.Name() {
			return nil
		}
	}

	if err := c.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(c.Name))
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
			index.Add(bucket, ref, Document{raw: doc})
		}

		return nil
	}); err != nil {
		return err
	}

	c.IndexList = append(c.IndexList, index)

	return nil
}

func (c *collection) DropIndex(name string) error {
	return c.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(c.Name))
		if err != nil {
			return err
		}

		var newIndices = make([]Index, len(c.IndexList))
		j := 0
		for _, i := range c.IndexList {
			if name == i.Name() {
				bucket.DeleteBucket(i.BucketName())
			} else {
				newIndices[j] = i
				j++
			}
		}
		c.IndexList = newIndices[:j]
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
	bucket, err := tx.CreateBucketIfNotExists([]byte(c.Name))
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
		for _, i := range c.IndexList {
			err = i.Add(bucket, ref, doc)
			if err != nil {
				return err
			}
		}

		err = docBucket.Put(ref, doc.raw)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *collection) Find(query Query) ([]Document, error) {
	docs := make([]Document, 0)
	walker := func(key Reference, value []byte) error {
		docs = append(docs, DocumentFromBytes(value))
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
	bucket := tx.Bucket([]byte(c.Name))
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
	for _, i := range c.IndexList {
		err = i.Delete(bucket, ref, doc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *collection) queryPlan(query Query) (queryPlan, error) {
	if query == nil {
		return nil, ErrNoQuery
	}

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
	if query == nil {
		return nil
	}

	// first map the indices to the number of matching search options
	var cIndex Index
	var cMatch float64

	for _, i := range c.IndexList {
		m := i.IsMatch(query)
		if m > cMatch {
			cIndex = i
			cMatch = m
		}
	}

	return cIndex
}

func (c *collection) Get(key Reference) (*Document, error) {
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

	return &Document{raw: data}, err
}

func (c *collection) documentBucket(tx *bbolt.Tx) *bbolt.Bucket {
	bucket := tx.Bucket([]byte(c.Name))
	if bucket == nil {
		return nil
	}
	return bucket.Bucket(documentCollectionByteRef())
}
