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

// Store is the main interface for storing/finding documents
type Store interface {
	// Collection creates or returns a jsonCollection.
	// On the db level it's a bucket for the documents and 1 bucket per index.
	Collection(name string) Collection
	// Close closes the bbolt DB
	Close() error
}

// Store holds a reference to the bbolt data file and all collections.
type store struct {
	db          *bbolt.DB
	collections map[string]*jsonCollection
}

// NewStore creates a new store.
// the noSync option disables flushing to disk, ideal for testing and bulk loading
func NewStore(dbFile string, noSync bool) (Store, error) {
	err := os.MkdirAll(filepath.Dir(dbFile), os.ModePerm)
	if err != nil {
		return nil, err
	}

	options := *bbolt.DefaultOptions
	options.NoSync = noSync

	db, err := bbolt.Open(dbFile, boltDBFileMode, &options)
	if err != nil {
		return nil, err
	}

	st := &store{
		db:          db,
		collections: map[string]*jsonCollection{},
	}

	return st, nil
}

func (s *store) Collection(name string) Collection {
	c, ok := s.collections[name]
	if !ok {
		c = &jsonCollection{
			name:    name,
			db:      s.db,
			refMake: defaultReferenceCreator,
		}
		s.collections[name] = c
	}

	return c
}

func (s *store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
