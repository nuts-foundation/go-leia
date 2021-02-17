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
	"encoding/hex"
	"errors"

	"go.etcd.io/bbolt"
)

// Collection defines a logical collection of documents and indices within a store.
type Collection interface {
	AddIndex(index Index) error
	DropIndex(name string) error

	Add(jsonSet []Document) error
	Get(ref Reference) (Document, error)
	Delete(doc Document) error

	// Find queries the collection for documents, it may return duplicates
	Find(query Query) (SearchResult, error)

	// Reference uses the configured reference function to generate a reference of the function
	Reference(doc Document) (Reference, error)
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
	Name      string   `json:"name"`
	db        *bbolt.DB
	IndexList []Index  `json:"indices"`
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

		cur := bucket.Cursor()
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
		bucket, err := tx.CreateBucketIfNotExists([]byte(c.Name))
		if err != nil {
			return err
		}

		for _, doc := range jsonSet {
			ref, err := c.refMake(doc)
			if err != nil {
				return err
			}

			err = bucket.Put(ref, doc)
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
		}

		return nil
	})
}

// SearchResult maps search keys to a set of documents
type SearchResult map[string][]Document

type ResultIterator func(key Key, doc Document) error

func (sr SearchResult) Iterate(iterator ResultIterator) error {
	for k, s := range sr {
		key, err := hex.DecodeString(k)
		if err != nil {
			return err
		}

		for _, d := range s {
			err = iterator(key, d)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (sr SearchResult) Add(key Key, doc Document) {
	if m, exists := sr[key.EncodeToHex()]; !exists {
		sr[key.EncodeToHex()] = []Document{doc}
	} else {
		sr[key.EncodeToHex()] = append(m, doc)
	}
}

func (c *collection) Find(query Query) (SearchResult, error) {
	sResult := SearchResult{}

	i := c.findIndex(query)

	if i == nil {
		return nil, errors.New("no index found")
	}

	err := c.db.View(func(tx *bbolt.Tx) error {
		// nil is not possible since adding an index creates the bucket
		bucket := tx.Bucket([]byte(c.Name))

		sr, err := i.Find(bucket, query)
		if err != nil {
			return err
		}

		err = sr.iterate(func(key Key, ref Reference) error {
			doc := bucket.Get(ref)
			if doc == nil {
				return errors.New("no document found for key")
			}
			sResult.Add(key, doc)

			return nil
		})
		return err
	})

	return sResult, err
}

// Delete a document from the store, this also removes the entries from indices
func (c *collection) Delete(doc Document) error {
	// find matching indices and remove hash from that index
	return c.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.Name))
		if bucket == nil {
			return nil
		}

		ref, err := c.refMake(doc)
		if err != nil {
			return err
		}
		err = bucket.Delete(ref)
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
	})
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
		}
	}

	return cIndex
}


// Get returns the data for the given key or nil if not found
func (c *collection) Get(key Reference) (Document, error) {
	var err error
	var data []byte

	err = c.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.Name))

		data = bucket.Get(key)
		return nil
	})

	return data, err
}
