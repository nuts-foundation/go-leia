/*
 * go-leia
 * Copyright (C) 2021 Nuts community
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package leia

import (
	"errors"
	"fmt"

	"go.etcd.io/bbolt"
)

// Store holds a reference to the bbolt data file and holds configured indices.
type Store struct {
	db      *bbolt.DB
	indices []Index
}

// NewStore creates a new store. The DB file is stored at the given path and configred with the given indices.
func NewStore(path string, indices ...Index) (*Store, error) {
	// todo: detect duplicate index names
	dbFile := fmt.Sprintf("%s/%s.db", path, documents)
	db, err := bbolt.Open(dbFile, boltDBFileMode, bbolt.DefaultOptions)
	if err != nil {
		return nil, err
	}

	store := &Store{db: db, indices: indices}
	if err := store.init(indices...); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *Store) init(indices ...Index) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(documents)); err != nil {
			return err
		}
		return nil
	})
}

// Add a json document set to the store
// this uses a single transaction per set.
func (s *Store) Add(jsonSet []Document) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(documents))

		for _, doc := range jsonSet {
			ref := NewReference(doc)
			err := bucket.Put(ref, doc)
			if err != nil {
				return err
			}

			// indices
			// buckets are cached within tx
			for _, i := range s.indices {
				err = i.Add(tx, doc)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (s *Store) Find(query Query) ([]Document, error) {
	var docs []Document

	i := s.findIndex(query)

	if i == nil {
		return nil, errors.New("no index found")
	}

	s.db.View(func(tx *bbolt.Tx) error {
		refs, err := i.Find(tx, query)
		if err != nil {
			return err
		}

		// todo change to collection name
		bucket := tx.Bucket([]byte("documents"))
		docs = make([]Document, len(refs))
		for i, r := range refs {
			docs[i] = bucket.Get(r)
		}
		return nil
	})

	return docs, nil
}

// Delete a document from the store, this also removes the entries from indices
func (s *Store) Delete(doc Document) error {
	// find matching indices and remove hash from that index
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(documents))

		h := doc.Reference()
		err := bucket.Delete(h)
		if err != nil {
			return err
		}

		// indices
		for _, i := range s.indices {
			err = i.Delete(tx, doc)
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
func (s *Store) findIndex(query Query) Index {
	// first map the indices to the number of matching search options
	var cIndex Index
	var cMatch float64

	for _, i := range s.indices {
		m := i.IsMatch(query)
		if m > cMatch {
			cIndex = i
		}
	}

	return cIndex
}

// Get returns the data for the given key or nil if not found
func (s *Store) Get(key []byte) ([]byte, error) {
	var err error
	var data []byte

	err = s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("documents"))

		data = bucket.Get(key)
		return nil
	})

	return data, err
}
