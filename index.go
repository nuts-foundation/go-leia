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

// Index describes an index. An index is based on a json path and has a path.
// The path is used for storage but also as identifier in search options.
type Index interface {
	// Name returns the path of this index
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
	// BucketName returns the bucket path for this index
	BucketName() []byte
	// QueryPartsOutsideIndex selects the queryParts that are not covered by the index.
	QueryPartsOutsideIndex(query Query) []QueryPart
	// Depth returns the number of indexed fields
	Depth() int
	// Keys returns the scalars found in the document at the location specified by the FieldIndexer
	Keys(fi FieldIndexer, document Document) ([]Scalar, error)
}

// iteratorFn defines a function that is used as a callback when an IterateIndex query finds results. The function is called for each result entry.
// the key will be the indexed value and the value will contain an Entry
type iteratorFn DocumentWalker

type index struct {
	name       string
	indexParts []FieldIndexer
	collection Collection
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
	return i.addDocumentR(cBucket, i.indexParts, Key{}, ref, doc)
}

// addDocumentR, like Add but recursive
func (i *index) addDocumentR(bucket *bbolt.Bucket, parts []FieldIndexer, cKey Key, ref Reference, doc Document) error {
	// current part
	ip := parts[0]

	matches, err := i.Keys(ip, doc)
	if err != nil {
		return err
	}

	// exit condition
	if len(parts) == 1 {
		// all matches to be added to current bucket
		for _, m := range matches {
			key := ComposeKey(cKey, m.Bytes())
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
		nKey := ComposeKey(cKey, m.Bytes())
		if err = i.addDocumentR(bucket, parts[1:], nKey, ref, doc); err != nil {
			return err
		}
	}

	// no matches for the document and this part of the index
	// add key with an empty byte slice as value
	if len(matches) == 0 {
		nKey := ComposeKey(cKey, []byte{})
		return i.addDocumentR(bucket, parts[1:], nKey, ref, doc)
	}

	return nil
}

// removeDocumentR, like Delete but recursive
func (i *index) removeDocumentR(bucket *bbolt.Bucket, parts []FieldIndexer, cKey Key, ref Reference, doc Document) error {
	// current part
	ip := parts[0]

	matches, err := i.Keys(ip, doc)
	if err != nil {
		return err
	}

	// exit condition
	if len(parts) == 1 {
		for _, m := range matches {
			key := ComposeKey(cKey, m.Bytes())
			_ = removeRefFromBucket(bucket, key, ref)
		}
		return nil
	}

	// continue recursion
	for _, m := range matches {
		nKey := ComposeKey(cKey, m.Bytes())
		return i.removeDocumentR(bucket, parts[1:], nKey, ref, doc)
	}

	// no matches for the document and this part of the index
	return nil
}

func (i *index) Delete(bucket *bbolt.Bucket, ref Reference, doc Document) error {
	cBucket := bucket.Bucket(i.BucketName())
	if cBucket == nil {
		return nil
	}

	return i.removeDocumentR(cBucket, i.indexParts, Key{}, ref, doc)
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

	parts := i.matchingParts(query)

outer:
	for thc, ip := range i.indexParts {
		for _, qp := range parts {
			if ip.Equals(qp) {
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

// matchingParts returns the queryParts that match the index.
// it also sorts them in the right order. If multiple matches exist a index position, the first is returned.
func (i *index) matchingParts(query Query) []QueryPart {
	var sorted = make([]QueryPart, len(i.indexParts))
outer:
	for _, qp := range query.parts {
		for j, ip := range i.indexParts {
			if ip.Equals(qp) {
				if sorted[j] == nil {
					sorted[j] = qp
					continue outer
				}
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
	return sorted
}

func (i *index) QueryPartsOutsideIndex(query Query) []QueryPart {
	matchingParts := i.matchingParts(query)
	resultingParts := make([]QueryPart, 0)
	visitedParts := make([]QueryPart, 0)

outer:
	for _, qp := range query.parts {
		for _, mp := range matchingParts {
			if mp.Equals(qp) {
				for _, hp := range visitedParts {
					if hp.Equals(qp) { // already excluded once
						resultingParts = append(resultingParts, qp)
						continue outer
					}
				}
				// exclude and continue
				visitedParts = append(visitedParts, mp)
				continue outer
			}
		}
		// no hit in index parts
		resultingParts = append(resultingParts, qp)
	}

	return resultingParts
}

func (i *index) Iterate(bucket *bbolt.Bucket, query Query, fn iteratorFn) error {
	var err error

	cBucket := bucket.Bucket(i.BucketName())
	if cBucket == nil {
		return err
	}

	// Sort the parts of the Query to conform to the index key building order
	sortedQueryParts := i.matchingParts(query)

	if len(sortedQueryParts) == 0 {
		return errors.New("unable to iterate over index without matching keys")
	}

	// extract tokenizer and transform to here
	matchers := i.matchers(sortedQueryParts)

	_, err = findR(cBucket.Cursor(), Key{}, matchers, fn, []byte{}, 0)
	return err
}

func (i *index) matchers(sortedQueryParts []QueryPart) []matcher {
	// extract tokenizer and transform to here
	matchers := make([]matcher, len(sortedQueryParts))
	for j, cPart := range sortedQueryParts {
		terms := make([]Scalar, 0)
		for _, token := range i.indexParts[j].Tokenize(cPart.Seek()) {
			seek := i.indexParts[j].Transform(token)
			terms = append(terms, seek)
		}
		matchers[j] = matcher{
			queryPart: cPart,
			terms:     terms,
			transform: i.indexParts[j].Transform,
		}
	}
	return matchers
}

func (i *index) Keys(j FieldIndexer, document Document) ([]Scalar, error) {
	// first get the raw values from the query path
	rawKeys, err := i.collection.ValuesAtPath(document, j.QueryPath())
	if err != nil {
		return nil, err
	}

	// run the tokenizer
	tokenized := make([]Scalar, 0)
	for _, rawKey := range rawKeys {
		tokens := j.Tokenize(rawKey)
		tokenized = append(tokenized, tokens...)
	}

	// run the transformer
	transformed := make([]Scalar, len(tokenized))
	for i, rawKey := range tokenized {
		transformed[i] = j.Transform(rawKey)
	}

	return transformed, nil
}

type matcher struct {
	queryPart QueryPart
	terms     []Scalar
	transform Transform
}

func findR(cursor *bbolt.Cursor, searchKey Key, matchers []matcher, fn iteratorFn, lastCursorPosition []byte, depth int) ([]byte, error) {
	var err error
	returnKey := lastCursorPosition
	currentQueryPart := matchers[0].queryPart
	//outer:
	for _, seekTerm := range matchers[0].terms {
		// new location in cursor to skip to
		seek := ComposeKey(searchKey, seekTerm.Bytes())
		condition := true

		// do not go back to prevent infinite loops. The cursor may only go forward.
		if bytes.Compare(seek, lastCursorPosition) < 0 {
			seek = lastCursorPosition
		}

		var currentKey []byte
		for currentKey, _ = cursor.Seek(seek); currentKey != nil && bytes.HasPrefix(currentKey, searchKey) && condition; {
			var newPart []byte
			split := Key(currentKey).Split()
			if len(split) >= depth+1 {
				newPart = split[depth]
			} // else use nil value

			// check of current (partial) key still matches with query
			condition = currentQueryPart.Condition(newPart, matchers[0].transform)
			if condition {
				if len(matchers) > 1 {
					// (partial) key still matches, continue to next index part
					nKey := ComposeKey(searchKey, newPart)
					// on success the cursor is moved forward, the latest key is returned, continue with that key
					// if keys haven't changed: break
					var subKey []byte
					subKey, err = findR(cursor, nKey, matchers[1:], fn, currentKey, depth+1)
					if bytes.Equal(subKey, currentKey) {
						// the nested search could not advance the cursor, so we do it here before continuing the loop
						currentKey, _ = cursor.Next()
						returnKey = currentKey
						continue
					}
					currentKey = subKey
				} else {
					// all index parts applied to key construction, retrieve results.
					err = iterateOverDocuments(cursor, currentKey, fn)
					// this position was a success, hopefully the next as well
					currentKey, _ = cursor.Next()
				}
				if err != nil {
					return nil, err
				}
			}
		}
		// move one lower?
		returnKey = currentKey
	}
	return returnKey, nil
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
