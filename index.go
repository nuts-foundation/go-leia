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
	"bytes"
	"errors"

	"go.etcd.io/bbolt"
)

// Index describes an index. An index is based on a json path and has a name.
// The name is used for storage but also as identifier in search options.
type Index interface {
	// Name returns the name of this index
	Name() string

	// Add indexes the document. It uses a sub-bucket of the given bucket.
	// It will only be indexed if the complete index matches.
	Add(bucket *bbolt.Bucket, ref Reference, doc Document) error

	// Delete document from the index
	Delete(bucket *bbolt.Bucket, ref Reference, doc Document) error

	// IsMatch determines if this index can be used for the given query. The higher the return value, the more likely it is useful.
	// return values lie between 0.0 and 1.0, where 1.0 is the most useful.
	IsMatch(query Query) float64

	// Iterate over the key/value pairs given a query. Entries that match the query are passed to the iteratorFn.
	// it will not filter out double values
	Iterate(bucket *bbolt.Bucket, query Query, fn iteratorFn) error

	// BucketName returns the bucket name for this index
	BucketName() []byte

	// Sort the query so its parts align with the index parts.
	// includeMissing, if true, the sort will append queryParts not matched by an index at the end.
	Sort(query Query, includeMissing bool) []QueryPart

	// QueryPartsOutsideIndex selects the queryParts that are not covered by the index.
	QueryPartsOutsideIndex(query Query) []QueryPart

	// Depth returns the number of indexed fields
	Depth() int
}

// iteratorFn defines a function that is used as a callback when an IterateIndex query finds results. The function is called for each result entry.
// the key will be the indexed value and the value will contain an Entry
type iteratorFn DocumentWalker

// NewIndex creates a new blank index.
// If multiple parts are given, a compound index is created.
func NewIndex(name string, parts ...FieldIndexer) Index {
	return &index{
		name:       name,
		indexParts: parts,
	}
}

// FieldIndexer is the public interface that defines functions for a field index instruction.
// A FieldIndexer is used when a document is indexed.
type FieldIndexer interface {
	// Name is used for matching against a Query
	Name() string
	// Keys returns the keys that matched this document. Multiple keys are combined by the index
	Keys(document Document) ([]Key, error)
	// Tokenize may split up Keys and search terms. For example split a sentence into words.
	Tokenize(value interface{}) []interface{}
	// Transform is a function that alters the value to be indexed as well as any search criteria.
	// For example LowerCase is a Transform function that transforms the value to lower case.
	Transform(value interface{}) interface{}
}

type index struct {
	name       string
	indexParts []FieldIndexer
}

func (i *index) Name() string {
	return i.name
}

func (i *index) BucketName() []byte {
	return []byte(i.Name())
}

func (i *index) Depth() int {
	return len(i.indexParts)
}

func (i *index) Add(bucket *bbolt.Bucket, ref Reference, doc Document) error {
	cBucket, _ := bucket.CreateBucketIfNotExists(i.BucketName())
	return addDocumentR(cBucket, i.indexParts, Key{}, ref, doc)
}

// addDocumentR, like Add but recursive
func addDocumentR(bucket *bbolt.Bucket, parts []FieldIndexer, cKey Key, ref Reference, doc Document) error {
	// current part
	ip := parts[0]

	matches, err := ip.Keys(doc)
	if err != nil {
		return err
	}

	// exit condition
	if len(parts) == 1 {
		// all matches to be added to current bucket
		for _, m := range matches {
			key := ComposeKey(cKey, m)
			_ = addRefToBucket(bucket, key, ref)
		}
		if len(matches) == 0 {
			key := ComposeKey(cKey, []byte{})
			_ = addRefToBucket(bucket, key, ref)
		}
		return nil
	}

	// continue recursion
	for _, m := range matches {
		nKey := ComposeKey(cKey, m)
		if err = addDocumentR(bucket, parts[1:], nKey, ref, doc); err != nil {
			return err
		}
	}

	// no matches for the document and this part of the index
	// add key with an empty byte slice as value
	if len(matches) == 0 {
		nKey := ComposeKey(cKey, []byte{})
		return addDocumentR(bucket, parts[1:], nKey, ref, doc)
	}

	return nil
}

// addDocumentR, like Add but recursive
func removeDocumentR(bucket *bbolt.Bucket, parts []FieldIndexer, cKey Key, ref Reference, doc Document) error {
	// current part
	ip := parts[0]

	matches, err := ip.Keys(doc)
	if err != nil {
		return err
	}

	// exit condition
	if len(parts) == 1 {
		for _, m := range matches {
			key := ComposeKey(cKey, m)
			_ = removeRefFromBucket(bucket, key, ref)
		}
		return nil
	}

	// continue recursion
	for _, m := range matches {
		nKey := ComposeKey(cKey, m)
		return removeDocumentR(bucket, parts[1:], nKey, ref, doc)
	}

	// no matches for the document and this part of the index
	return nil
}

func (i *index) Delete(bucket *bbolt.Bucket, ref Reference, doc Document) error {
	cBucket := bucket.Bucket(i.BucketName())
	if cBucket == nil {
		return nil
	}

	return removeDocumentR(cBucket, i.indexParts, Key{}, ref, doc)
}

// addRefToBucket adds the reference to the correct key in the bucket. It handles multiple reference on the same location
func addRefToBucket(bucket *bbolt.Bucket, key Key, ref Reference) error {
	// first check if there's a sub-bucket
	subBucket, err := bucket.CreateBucketIfNotExists(key)
	if err != nil {
		return err
	}
	return subBucket.Put(ref, []byte{})
}

// removeRefFromBucket removes the reference from the bucket. It handles multiple reference on the same location
func removeRefFromBucket(bucket *bbolt.Bucket, key Key, ref Reference) error {
	// first check if there's a sub-bucket
	subBucket := bucket.Bucket(key)
	if subBucket == nil {
		return nil
	}
	return subBucket.Delete(ref)
}

func (i *index) IsMatch(query Query) float64 {
	hitcount := 0

	parts := i.Sort(query, false)

outer:
	for thc, ip := range i.indexParts {
		for _, qp := range parts {
			if ip.Name() == qp.Name() {
				hitcount++
			}
		}
		// if a miss is encountered, do not continue. You can't skip an index lvl
		if hitcount == thc {
			break outer
		}
	}

	return float64(hitcount) / float64(len(i.indexParts))
}

func (i *index) Sort(query Query, includeMissing bool) []QueryPart {
	var sorted = make([]QueryPart, len(i.indexParts))

outer:
	for _, qp := range query.Parts() {
		for j, ip := range i.indexParts {
			if ip.Name() == qp.Name() {
				sorted[j] = qp
				continue outer
			}
		}
	}

	// only use till the first nil value
	for i, s := range sorted {
		if s == nil {
			sorted = sorted[:i]
			break
		}
	}

	if includeMissing {
		// now include all params not in the sorted list
	outerMissing:
		for _, qp := range query.Parts() {
			for _, sp := range sorted {
				if sp.Name() == qp.Name() {
					continue outerMissing
				}
			}
			// missing so append
			sorted = append(sorted, qp)
		}
	}

	return sorted
}

func (i *index) QueryPartsOutsideIndex(query Query) []QueryPart {
	hits := 0
	parts := i.Sort(query, true)

	for j, qp := range parts {
		if j >= len(i.indexParts) || qp.Name() != i.indexParts[j].Name() {
			break
		}
		hits++
	}

	if hits == len(parts) {
		return []QueryPart{}
	}

	return parts[hits:]
}

func (i *index) Iterate(bucket *bbolt.Bucket, query Query, fn iteratorFn) error {
	var err error

	cBucket := bucket.Bucket(i.BucketName())
	if cBucket == nil {
		return err
	}

	// Sort the parts of the Query to conform to the index key building order
	sortedQueryParts := i.Sort(query, false)

	if len(sortedQueryParts) == 0 {
		return errors.New("unable to iterate over index without matching keys")
	}

	// extract tokenizer and transform to here
	matchers := make([]matcher, len(sortedQueryParts))
	for j, cPart := range sortedQueryParts {
		terms := make([]Key, 0)
		seek, err := cPart.Seek()
		if err != nil {
			return err
		}
		for _, token := range i.indexParts[j].Tokenize(seek) {
			seek = KeyOf(i.indexParts[j].Transform(token))
			terms = append(terms, seek)
		}
		matchers[j] = matcher{
			queryPart: cPart,
			terms:     terms,
			transform: i.indexParts[j].Transform,
		}
	}

	return findR(cBucket.Cursor(), Key{}, matchers, fn)
}

type matcher struct {
	queryPart QueryPart
	terms     []Key
	transform Transform
}

func findR(cursor *bbolt.Cursor, sKey Key, matchers []matcher, fn iteratorFn) error {
	var err error
	cPart := matchers[0].queryPart
	for _, seek := range matchers[0].terms {
		seek = ComposeKey(sKey, seek)
		condition := true
		for cKey, _ := cursor.Seek(seek); cKey != nil && bytes.HasPrefix(cKey, sKey) && condition; cKey, _ = cursor.Next() {
			// remove prefix (+1), Split and take first
			pf := cKey[len(sKey)+1:]
			if len(sKey) == 0 {
				pf = cKey
			}
			pfk := Key(pf)
			newp := pfk.Split()[0] // todo bounds check?

			// check of current (partial) key still matches with query
			condition, err = cPart.Condition(newp, matchers[0].transform)
			if err != nil {
				return err
			}
			if condition {
				if len(matchers) > 1 {
					// (partial) key still matches, continue to next index part
					nKey := ComposeKey(sKey, newp)
					err = findR(cursor, nKey, matchers[1:], fn)
				} else {
					// all index parts applied to key construction, retrieve results.
					err = iterateOverDocuments(cursor, cKey, fn)
				}
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func iterateOverDocuments(cursor *bbolt.Cursor, cKey []byte, fn iteratorFn) error {
	subBucket := cursor.Bucket().Bucket(cKey)
	if subBucket != nil {
		subCursor := subBucket.Cursor()
		for k, _ := subCursor.Seek([]byte{}); k != nil; k, _ = subCursor.Next() {
			if err := fn(cKey, k); err != nil {
				return err
			}
		}
	}
	return nil
}
