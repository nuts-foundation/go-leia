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
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

// GlobalCollection is the collection that stores all the documents
// specific collection only store indices
const GlobalCollection = "_global"

// Store is the main interface for storing/finding documents
type Store interface {
	// Collection creates or returns a collection.
	// On the db level it's a bucket for the documents and 1 bucket per index.
	Collection(name string) Collection
}

// Store holds a reference to the bbolt data file and holds configured indices per collection.
type store struct {
	db               *bbolt.DB
	globalCollection *collection
	indices          []Index
	collections      map[string]*collection
}

// NewStore creates a new store.
func NewStore(dbFile string) (Store, error) {
	err := os.MkdirAll(filepath.Dir(dbFile), os.ModePerm)
	if err != nil {
		return nil, err
	}

	db, err := bbolt.Open(dbFile, boltDBFileMode, bbolt.DefaultOptions)
	if err != nil {
		return nil, err
	}

	st := &store{
		db:          db,
		collections: map[string]*collection{},
	}

	st.globalCollection = &collection{
		Name:    GlobalCollection,
		db:      st.db,
		refMake: defaultReferenceCreator,
	}
	st.globalCollection.globalCollection = st.globalCollection
	return st, nil
}

func (s *store) Collection(name string) Collection {
	if name == GlobalCollection {
		return s.globalCollection
	}

	c, ok := s.collections[name]
	if !ok {
		c = &collection{
			Name:             name,
			db:               s.db,
			globalCollection: s.globalCollection,
			refMake:          defaultReferenceCreator,
		}
		s.collections[name] = c
	}

	return c
}
