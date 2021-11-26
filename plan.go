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

// queryPlan is the interface for all query plans
type queryPlan interface {
	// execute the plan call the DocumentWalker for each matching document
	execute(walker DocumentWalker) error
}

// queryPlanBase contains elements common for each query plan
type queryPlanBase struct {
	collection *collection
	query      Query
}

// fullTableScanQueryPlan is a query plan which scans all documents
type fullTableScanQueryPlan struct {
	queryPlanBase
}

// resultScanQueryPlan is a query plan that uses an index and filters the results with the remaining query params
type resultScanQueryPlan struct {
	queryPlanBase
	index Index
}

// indexScanQueryPlan is a special query plan that only loops over the index keys and document references
type indexScanQueryPlan struct {
	queryPlanBase
	index Index
}

// ReferenceScanFn is a function type which is called with an index key and a document Reference as value
type ReferenceScanFn func(key []byte, value []byte) error

// documentScanFn is a function type which is called with a document Reference as key and a the document bytes as value
type documentScanFn func(key []byte, value []byte) error

func (f fullTableScanQueryPlan) execute(walker DocumentWalker) error {
	return f.collection.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(f.collection.Name))
		if bucket == nil {
			// no bucket means no docs
			return nil
		}
		bucket = bucket.Bucket(documentCollectionByteRef())
		if bucket == nil {
			// no bucket means no docs
			return nil
		}

		parts := make([]QueryPart, 0)
		if f.query != nil {
			parts = f.query.Parts()
		}
		scanner := resultScanner(parts, walker)

		cursor := bucket.Cursor()
		for ref, bytes := cursor.First(); bytes != nil; ref, bytes = cursor.Next() {
			if err := scanner(ref, bytes); err != nil {
				return err
			}
		}
		return nil
	})
}

func (i indexScanQueryPlan) execute(walker ReferenceScanFn) error {
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
		if iBucket == nil { // nothing added yet
			return nil
		}

		// expander expands the index entry to the actual document
		expander := indexEntryExpander(walker)

		return i.index.Iterate(iBucket, i.query, expander)
	})
}

func (i resultScanQueryPlan) execute(walker DocumentWalker) error {
	queryParts, err := i.index.QueryPartsOutsideIndex(i.query)
	if err != nil {
		return err
	}

	// do the IndexScan
	return i.collection.db.View(func(tx *bbolt.Tx) error {
		docBucket := i.collection.documentBucket(tx)
		if docBucket == nil {
			// no bucket means no docs
			return nil
		}

		// nil is not possible since adding an index creates the iBucket
		iBucket := tx.Bucket([]byte(i.collection.Name))

		// resultScanner takes the refs from the indexScan, resolves the document and applies the remaining queryParts
		resultScan := resultScanner(queryParts, walker)

		// fetcher expands references to documents, for each document it calls the resultScan
		fetcher := documentFetcher(docBucket, resultScan)

		// expander expands the index entry to the actual document
		expander := indexEntryExpander(fetcher)

		return i.index.Iterate(iBucket, i.query, expander)
	})
}

// documentFetcher creates a ReferenceScanFn which is called with a reference, fetches the document and calls the documentScanFn
func documentFetcher(documentCollection *bbolt.Bucket, docWalker documentScanFn) ReferenceScanFn {
	return func(key []byte, ref []byte) error {
		if documentCollection == nil {
			return nil
		}
		docBytes := documentCollection.Get(ref)
		if docBytes != nil {
			return docWalker(ref, docBytes)
		}
		return nil
	}

}

// resultScanner returns a resultScannerFn. For each call it will compare the document against the given queryParts.
// If conditions are met, it'll call the DocumentWalker
func resultScanner(queryParts []QueryPart, walker DocumentWalker) documentScanFn {
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
	// refMap contains references that have already been processed
	refMap := map[string]bool{}

	return func(key Reference, value []byte) error {
		ref := Reference(value)
		if _, b := refMap[ref.EncodeToString()]; !b {
			refMap[ref.EncodeToString()] = true
			if err := refScan(key, ref); err != nil {
				return err
			}
		}
		return nil
	}
}
