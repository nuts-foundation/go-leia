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

// CollectionType defines if a Collection is a JSON collection or JSONLD collection.
type CollectionType int

const (
	// JSONCollection defines a collection uses JSON search paths to index documents
	JSONCollection CollectionType = iota
	// JSONLDCollection defines a collection uses JSON-LD IRI search paths to index documents
	JSONLDCollection
)

// Store is the main interface for storing/finding documents
type Store interface {
	// Collection creates or returns a Collection of the specified type.
	// On the db level it's a bucket for the documents and 1 bucket per index.
	Collection(collectionType CollectionType, name string) Collection
	// Close the bbolt DB
	Close() error
}

// Store holds a reference to the bbolt data file and all collections.
type store struct {
	db             *bbolt.DB
	collections    map[string]*collection
	documentLoader ld.DocumentLoader
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
		options:        *bbolt.DefaultOptions,
		collections:    map[string]*collection{},
		documentLoader: ld.NewDefaultDocumentLoader(nil),
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

func (s *store) Collection(collectionType CollectionType, name string) Collection {
	c, ok := s.collections[name]
	if !ok {
		var vCollector valueCollector
		switch collectionType {
		case JSONCollection:
			vCollector = JSONPathValueCollector
		case JSONLDCollection:
			vCollector = JSONLDValueCollector
		default:
			panic("unknown collection type")
		}
		c = &collection{
			name:           name,
			collectionType: collectionType,
			db:             s.db,
			documentLoader: s.documentLoader,
			refMake:        defaultReferenceCreator,
			valueCollector: vCollector,
		}
		s.collections[name] = c
	} else if c.collectionType != collectionType {
		panic("collection already exists with different type")
	}

	return c
}
func (s *store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
