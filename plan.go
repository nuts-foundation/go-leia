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

// documentScanFn is a function type which is called with a document Reference as key and the document bytes as value
type documentScanFn func(key []byte, value []byte) error

func (f fullTableScanQueryPlan) execute(walker DocumentWalker) error {
	var docsScanned, docsMatched, docsScannedBytes, docsMatchedBytes int

	err := f.collection.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(f.collection.name))
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
		if f.query.parts != nil {
			parts = f.query.parts
		}

		// Wrap walker to count matched documents and bytes
		wrappedWalker := func(ref Reference, doc []byte) error {
			docsMatched++
			docsMatchedBytes += len(doc)
			return walker(ref, doc)
		}
		scanner := resultScanner(parts, wrappedWalker, f.collection)

		cursor := bucket.Cursor()
		for ref, bytes := cursor.First(); bytes != nil; ref, bytes = cursor.Next() {
			docsScanned++
			docsScannedBytes += len(bytes)
			if err := scanner(ref, bytes); err != nil {
				return err
			}
		}
		return nil
	})

	// Call callback if configured (only when query has conditions - empty query is intentional scan-all)
	if err == nil && len(f.query.parts) > 0 && f.collection.queryStatsCallbacks.OnIndexProblem != nil {
		f.collection.queryStatsCallbacks.OnIndexProblem(IndexStats{
			Collection:            f.collection.name,
			Query:                 f.query,
			DocumentsScanned:      docsScanned,
			DocumentsScannedBytes: docsScannedBytes,
			DocumentsMatched:      docsMatched,
			DocumentsMatchedBytes: docsMatchedBytes,
			SuggestedFields:       suggestIndexFields(f.query.parts),
			IndexUsed:             "",
			FilterEfficiency:      0.0,
		})
	}

	return err
}

func (i indexScanQueryPlan) execute(walker ReferenceScanFn) error {
	queryParts := i.index.QueryPartsOutsideIndex(i.query)
	if len(queryParts) != 0 {
		return errors.New("no index with exact match to query found")
	}

	// do the IndexScan
	return i.collection.db.View(func(tx *bbolt.Tx) error {
		// nil is not possible since adding an index creates the iBucket
		iBucket := tx.Bucket([]byte(i.collection.name))
		if iBucket == nil { // nothing added yet
			return nil
		}

		// expander expands the index entry to the actual document
		expander := indexEntryExpander(walker)

		return i.index.Iterate(iBucket, i.query, expander)
	})
}

func (i resultScanQueryPlan) execute(walker DocumentWalker) error {
	queryParts := i.index.QueryPartsOutsideIndex(i.query)
	var docsScanned, docsMatched, docsScannedBytes, docsMatchedBytes int

	// do the IndexScan
	err := i.collection.db.View(func(tx *bbolt.Tx) error {
		docBucket := i.collection.documentBucket(tx)
		if docBucket == nil {
			// no bucket means no docs
			return nil
		}

		// nil is not possible since adding an index creates the iBucket
		iBucket := tx.Bucket([]byte(i.collection.name))

		// Wrap walker to count matched documents and bytes
		wrappedWalker := func(ref Reference, doc []byte) error {
			docsMatched++
			docsMatchedBytes += len(doc)
			return walker(ref, doc)
		}

		// resultScanner takes the refs from the indexScan, resolves the document and applies the remaining queryParts
		resultScan := resultScanner(queryParts, wrappedWalker, i.collection)

		// Fetch document once and reuse for both counting and scanning (avoids double DB lookup)
		fetcherWithCounter := func(key []byte, ref []byte) error {
			if docBucket == nil {
				return nil
			}
			docBytes := docBucket.Get(ref)
			if docBytes == nil {
				return nil
			}

			// Count scanned bytes
			docsScanned++
			docsScannedBytes += len(docBytes)

			// Pass the already-fetched document to the scanner
			return resultScan(ref, docBytes)
		}

		// expander expands the index entry to the actual document
		expander := indexEntryExpander(fetcherWithCounter)

		return i.index.Iterate(iBucket, i.query, expander)
	})

	// Call callback if configured and threshold is met
	if err == nil && i.collection.queryStatsCallbacks.OnIndexProblem != nil {
		threshold := i.collection.queryStatsCallbacks.SuboptimalIndexThreshold
		// Default threshold is 3 wasted scans (scanned but not matched documents) if set to a negative value
		if threshold < 0 {
			threshold = 3
		}

		wastedScans := docsScanned - docsMatched
		if wastedScans > threshold {
			efficiency := 1.0
			if docsScanned > 0 {
				efficiency = float64(docsMatched) / float64(docsScanned)
			}

			i.collection.queryStatsCallbacks.OnIndexProblem(IndexStats{
				Collection:            i.collection.name,
				Query:                 i.query,
				DocumentsScanned:      docsScanned,
				DocumentsScannedBytes: docsScannedBytes,
				DocumentsMatched:      docsMatched,
				DocumentsMatchedBytes: docsMatchedBytes,
				SuggestedFields:       suggestIndexFields(queryParts),
				IndexUsed:             i.index.Name(),
				FilterEfficiency:      efficiency,
			})
		}
	}

	return err
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
func resultScanner(queryParts []QueryPart, walker DocumentWalker, collection *collection) documentScanFn {
	return func(ref []byte, doc []byte) error {
	outer:
		for _, part := range queryParts {
			keys, err := collection.ValuesAtPath(doc, part.QueryPath())
			if err != nil {
				return err
			}
			for _, k := range keys {
				if part.Condition(k.Bytes(), nil) {
					continue outer
				}
			}
			return nil
		}
		return walker(ref, doc)
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
