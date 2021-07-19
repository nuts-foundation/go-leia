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
	"errors"

	"go.etcd.io/bbolt"
)

type queryPlan interface {
	Execute(walker DocWalker) error
}

type defaultQueryPlan struct {
	collection *collection
}

type fullTableScanQueryPlan struct {
	defaultQueryPlan
	queryParts []QueryPart
}

func (f fullTableScanQueryPlan) Execute(walker DocWalker) error {
	return f.collection.globalCollection.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(GlobalCollection))
		if bucket == nil {
			// no bucket means no docs
			return nil
		}

		scanner := resultScanner(f.queryParts, walker)

		cursor := bucket.Cursor()
		for ref, bytes := cursor.First(); bytes != nil; ref, bytes = cursor.Next() {
			if err := scanner(ref, bytes); err != nil {
				return err
			}
		}
		return nil
	})
}

type indexScanQueryPlan struct {
	defaultQueryPlan
	index Index
	query Query
}

func (i indexScanQueryPlan) Execute(walker ReferenceScanFn) error {
	queryParts, err := i.index.QueryPartsOutsideIndex(i.query)
	if err != nil {
		return err
	}
	if len(queryParts) != 0 {
		return errors.New("no index with exact match to query found")
	}

	// do the IndexScan
	return i.collection.db.View(func(tx *bbolt.Tx) error {
		// nil is not possible since adding an index creates the iBucket
		iBucket := tx.Bucket([]byte(i.collection.Name))

		// expander expands the index entry to the actual document
		expander := indexEntryExpander(walker)

		return i.index.Iterate(iBucket, i.query, expander)
	})
}

type resultScanQueryPlan struct {
	defaultQueryPlan
	index Index
	query Query
}

func (i resultScanQueryPlan) Execute(walker DocWalker) error {
	queryParts, err := i.index.QueryPartsOutsideIndex(i.query)
	if err != nil {
		return err
	}

	// do the IndexScan
	return i.collection.db.View(func(tx *bbolt.Tx) error {
		globalBucket := tx.Bucket([]byte(GlobalCollection))
		if globalBucket == nil {
			// no bucket means no docs
			return nil
		}

		// nil is not possible since adding an index creates the iBucket
		iBucket := tx.Bucket([]byte(i.collection.Name))

		// resultScanner takes the refs from the indexScan, resolves the document and applies the remaining queryParts
		resultScan := resultScanner(queryParts, walker)

		// fetcher expands references to documents, for each document it calls the resultScan
		fetcher := documentFetcher(globalBucket, resultScan)

		// expander expands the index entry to the actual document
		expander := indexEntryExpander(fetcher)

		return i.index.Iterate(iBucket, i.query, expander)
	})
}

// ReferenceScanFn is a function type which is called with an index key and a document Reference as value
type ReferenceScanFn func(key []byte, value []byte) error

// documentScanFn is a function type which is called with a document Reference as key and a the document bytes as value
type documentScanFn func(key []byte, value []byte) error

// documentFetcher creates a ReferenceScanFn which is called with a reference, fetches the document and calls the documentScanFn
func documentFetcher(globalCollection *bbolt.Bucket, docWalker documentScanFn) ReferenceScanFn {
	return func(key []byte, ref []byte) error {
		docBytes := globalCollection.Get(ref)
		if docBytes != nil {
			return docWalker(ref, docBytes)
		}
		return nil
	}

}

// resultScanner returns a resultScannerFn. For each call it will compare the document against the given queryParts.
// If conditions are met, it'll call the DocWalker
func resultScanner(queryParts []QueryPart, walker DocWalker) documentScanFn {
	return func(ref []byte, docBytes []byte) error {
		doc := DocumentFromBytes(docBytes)
	outer:
		for _, part := range queryParts {
			keys, err := doc.KeysAtPath(part.Name())
			if err != nil {
				return err
			}
			for _, k := range keys {
				m, err := part.Condition(k, nil)
				if err != nil {
					return err
				}
				if m {
					continue outer
				}
			}
			return nil
		}
		return walker(ref, doc.raw)
	}
}

// indexEntryExpander creates a iteratorFn that expands an index Entry into multiple document references.
// for each reference the ReferenceScanFn func is called.
func indexEntryExpander(refScan ReferenceScanFn) iteratorFn {
	// contains references that have already been processed
	refMap := map[string]bool{}

	return func(key []byte, value []byte) error {
		refs, err := entryToSlice(value)
		if err != nil {
			return err
		}
		for _, r := range refs {
			if _, b := refMap[r.EncodeToString()]; !b {
				refMap[r.EncodeToString()] = true
				if err := refScan(key, r); err != nil {
					return err
				}
			}
		}
		return nil
	}
}
