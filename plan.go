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

import "go.etcd.io/bbolt"

type queryPlan interface {
	Execute(walker DocWalker) error
}

type defaultQueryPlan struct {
	collection *collection
}

type fullTableScanQueryPlan struct {
	defaultQueryPlan
	matchers   []matcher
}

func (f fullTableScanQueryPlan) Execute(walker DocWalker) error {
	return f.collection.globalCollection.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(f.collection.globalCollection.Name))
		if bucket == nil {
			// no bucket means no docs
			return nil
		}

		cursor := bucket.Cursor()
		outer:
		for ref, bytes := cursor.First(); bytes != nil; ref, bytes = cursor.Next() {
			doc := Document{raw: bytes}
			inner:
			for _, m := range f.matchers {
				keys, err := doc.KeysAtPath(m.queryPart.Name())
				if err != nil {
					return err
				}
				for _, key := range keys {
					condition, err := m.queryPart.Condition(key, nil)
					if err != nil {
						return err
					}
					if condition {
						// this matcher is successful. Continue with next matcher
						continue inner
					}
				}
				// no key matched for this mather, continue to next doc
				continue outer
			}
			if err := walker(ref, bytes); err != nil {
				return err
			}
		}
		return nil
	})
}
// todo a selector: returns partials per doc

type resultScanQueryPlan struct {
	defaultQueryPlan
	index Index
	query Query
}

func (i resultScanQueryPlan) Execute(walker DocWalker) error {
	sortedQueryParts, err := i.index.Sort(i.query, true)
	if err != nil {
		return err
	}

	// do the IndexScan
	return i.collection.db.View(func(tx *bbolt.Tx) error {
		// nil is not possible since adding an index creates the iBucket
		iBucket := tx.Bucket([]byte(i.collection.Name))

		// resultScan checks if the document conforms to the filters
		resultScan := func(key []byte, ref []byte) error {
			doc, err := i.collection.globalCollection.Get(ref)
			if err != nil {
				return err
			}
			if doc != nil {
				match := true
				outer:
				for _, part := range sortedQueryParts[i.index.Depth():] {
					// name must equal the json path for an unknown query part
					ip := fieldIndexer{path: part.Name()}
					keys, err := ip.Keys(*doc)
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
					match = false
				}
				if match {
					walker(ref, doc.raw)
				}
			}
			return nil
		}

		// contains references that have already been processed
		refMap := map[string]bool{}

		// collector expands the index entry to the actual document
		collector := func(key []byte, value []byte) error {
			refs, err := entryToSlice(value)
			if err != nil {
				return err
			}
			for _, r := range refs {
				if _, b := refMap[r.EncodeToString()]; !b {
					refMap[r.EncodeToString()] = true
					resultScan(key, r)
				}
			}
			return nil
		}

		return i.index.Iterate(iBucket, i.query, collector)
	})
}

