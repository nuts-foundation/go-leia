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
	"crypto/sha256"
	"errors"

	"go.etcd.io/bbolt"
)

// ErrNoIndex is returned when no index is found to query against
var ErrNoIndex = errors.New("no index found")

// Collection defines a logical collection of documents and indices within a store.
type Collection interface {
	// AddIndex to this collection. It doesn't matter if the index already exists.
	// If you want to override an index (by name) drop it first.
	AddIndex(index Index) error
	// DropIndex by name
	DropIndex(name string) error
	// Add a set of documents to this collection
	Add(jsonSet []Document) error
	// Get returns a document by reference
	Get(ref Reference) (Document, error)
	// Delete a document
	Delete(doc Document) error
	// Find queries the collection for documents
	// returns ErrNoIndex when no suitable index can be found
	Find(query Query) ([]Document, error)
	// Reference uses the configured reference function to generate a reference of the function
	Reference(doc Document) (Reference, error)
	// Iterate over matching key/value pairs.
	// returns ErrNoIndex when no suitable index can be found
	Iterate(query Query, fn IteratorFn) error
}

// ReferenceFunc is the func type used for creating references.
type ReferenceFunc func(doc Document) (Reference, error)

// default for shasum docs
func defaultReferenceCreator(doc Document) (Reference, error) {
	s := sha256.Sum256(doc)
	var b = make([]byte, 32)
	copy(b, s[:])

	return b, nil
}

type collection struct {
	Name             string `json:"name"`
	db               *bbolt.DB
	globalCollection *collection
	IndexList        []Index `json:"indices"`
	refMake          ReferenceFunc
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

		gBucket, err := tx.CreateBucketIfNotExists([]byte(GlobalCollection))
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

func (c *collection) Reference(doc Document) (Reference, error) {
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

	for _, doc := range jsonSet {
		ref, err := c.refMake(doc)
		if err != nil {
			return err
		}

		// indices
		// buckets are cached within tx
		for _, i := range c.IndexList {
			err = i.Add(bucket, ref, doc)
			if err != nil {
				return err
			}
		}

		if c.isGlobal() {
			bucket.Put(ref, doc)
		}
	}

	if c.isNotGlobal() {
		err = c.globalCollection.add(tx, jsonSet)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *collection) isNotGlobal() bool {
	return c != c.globalCollection && c.globalCollection != nil
}

func (c *collection) isGlobal() bool {
	return c.Name == GlobalCollection
}

func (c *collection) Find(query Query) ([]Document, error) {
	var docs []Document

	i := c.findIndex(query)
	if i == nil {
		return nil, ErrNoIndex
	}

	// the iteratorFn that collects results in a slice
	refMap := map[string]bool{}
	var newRef = make([]Reference, 0)
	var refFn = func(key []byte, value []byte) error {
		refs, err := entryToSlice(value)
		if err != nil {
			return err
		}
		for _, r := range refs {
			if _, b := refMap[r.EncodeToString()]; !b {
				refMap[r.EncodeToString()] = true
				newRef = append(newRef, r)
			}
		}
		return nil
	}

	if err := c.Iterate(query, refFn); err != nil {
		return nil, err
	}

	err := c.db.View(func(tx *bbolt.Tx) (err error) {
		docs = make([]Document, len(newRef))
		for i, r := range newRef {
			docs[i], err = c.globalCollection.Get(r)
			if err != nil {
				return
			}
		}
		return
	})

	return docs, err
}

func (c *collection) Iterate(query Query, fn IteratorFn) error {
	i := c.findIndex(query)

	if i == nil {
		return ErrNoIndex
	}

	return c.db.View(func(tx *bbolt.Tx) error {
		// nil is not possible since adding an index creates the iBucket
		iBucket := tx.Bucket([]byte(c.Name))

		return i.Iterate(iBucket, query, fn)
	})
}

// Delete a document from the store, this also removes the entries from indices
func (c *collection) Delete(doc Document) error {
	// find matching indices and remove hash from that index
	return c.db.Update(func(tx *bbolt.Tx) error {
		return c.delete(tx, doc)
	})
}

func (c *collection) delete(tx *bbolt.Tx, doc Document) error {
	iBucket := tx.Bucket([]byte(c.Name))
	if iBucket == nil {
		return nil
	}

	ref, err := c.refMake(doc)
	if err != nil {
		return err
	}

	if c.isNotGlobal() {
		err = c.globalCollection.delete(tx, doc)
		if err != nil {
			return err
		}
	}

	// indices
	for _, i := range c.IndexList {
		err = i.Delete(iBucket, ref, doc)
		if err != nil {
			return err
		}
	}

	return nil
}

// find a matching index.
// The index may, at most, be one longer than the number of search options.
// The longest index will win.
func (c *collection) findIndex(query Query) Index {
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

// Get returns the data for the given key or nil if not found
func (c *collection) Get(key Reference) (Document, error) {
	var err error
	var data []byte

	err = c.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(GlobalCollection))

		data = bucket.Get(key)
		return nil
	})

	return data, err
}
