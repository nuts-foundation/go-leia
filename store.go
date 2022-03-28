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

	"github.com/piprate/json-gold/ld"
	"go.etcd.io/bbolt"
)

// Store is the main interface for storing/finding documents
type Store interface {
	// JSONCollection creates or returns a JSON Collection.
	// On the db level it's a bucket for the documents and 1 bucket per index.
	JSONCollection(name string) Collection
	// JSONLDCollection creates or returns a JSON-LD Collection.
	// On the db level it's a bucket for the documents and 1 bucket per index.
	JSONLDCollection(name string) Collection
	// Close the bbolt DB
	Close() error
}

// Store holds a reference to the bbolt data file and all collections.
type store struct {
	db                *bbolt.DB
	collections       map[string]Collection
	documentLoader    ld.DocumentLoader
	documentProcessor *ld.JsonLdProcessor
	// options is used during configuration
	options bbolt.Options
}

// StoreOption is the function type for the Store Options
type StoreOption func(store *store)

// WithoutSync is a store option which signals the underlying bbolt db to skip syncing with disk
func WithoutSync() StoreOption {
	return func(store *store) {
		store.options.NoSync = true
	}
}

// WithDocumentLoader overrides the default document loader
func WithDocumentLoader(documentLoader ld.DocumentLoader) StoreOption {
	return func(store *store) {
		store.documentLoader = documentLoader
	}

}

// NewStore creates a new store.
// the noSync option disables flushing to disk, ideal for testing and bulk loading
func NewStore(dbFile string, options ...StoreOption) (Store, error) {
	err := os.MkdirAll(filepath.Dir(dbFile), os.ModePerm)
	if err != nil {
		return nil, err
	}

	// store with defaults
	st := &store{
		options:           *bbolt.DefaultOptions,
		collections:       map[string]Collection{},
		documentLoader:    ld.NewDefaultDocumentLoader(nil),
		documentProcessor: ld.NewJsonLdProcessor(),
	}

	// apply options
	for _, option := range options {
		option(st)
	}

	st.db, err = bbolt.Open(dbFile, boltDBFileMode, &st.options)
	if err != nil {
		return nil, err
	}

	return st, nil
}

func (s *store) JSONCollection(name string) Collection {
	c, ok := s.collections[name]
	if !ok {
		c = &collection{
			name:           name,
			db:             s.db,
			refMake:        defaultReferenceCreator,
			valueCollector: JSONPathValueCollector,
		}
		s.collections[name] = c
	}

	return c
}

func (s *store) JSONLDCollection(name string) Collection {
	c, ok := s.collections[name]
	if !ok {
		c = &collection{
			name:              name,
			db:                s.db,
			refMake:           defaultReferenceCreator,
			documentProcessor: s.documentProcessor,
			valueCollector:    JSONLDValueCollector,
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
