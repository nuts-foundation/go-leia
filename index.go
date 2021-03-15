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

	// Find the references matching the query
	Find(bucket *bbolt.Bucket, query Query) ([]Reference, error)

	// BucketName returns the bucket name for this index
	BucketName() []byte
}

// NewIndex creates a new blank index.
// If multiple parts are given, a compound index is created.
// This index is only useful if at least n-1 parts are used in the query.
func NewIndex(name string, parts ...IndexPart) Index {
	return &index{
		name:       name,
		indexParts: parts,
	}
}

type IndexPart interface {
	// for matching against a Query
	Name() string
	// Keys returns the keys that matched this document. Multiple keys are combined by the index
	Keys(document Document) ([]Key, error)
}

type index struct {
	name       string
	indexParts []IndexPart
}

func (i *index) Name() string {
	return i.name
}

func (i *index) BucketName() []byte {
	return []byte(i.Name())
}

func (i *index) Add(bucket *bbolt.Bucket, ref Reference, doc Document) error {
	cBucket, _ := bucket.CreateBucketIfNotExists(i.BucketName())
	return addDocumentR(cBucket, i.indexParts, Key{}, ref, doc)
}

// addDocumentR, like Add but recursive
func addDocumentR(bucket *bbolt.Bucket, parts []IndexPart, cKey Key, ref Reference, doc Document) error {
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
	return nil
}

// addDocumentR, like Add but recursive
func removeDocumentR(bucket *bbolt.Bucket, parts []IndexPart, cKey Key, ref Reference, doc Document) error {
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
	entryBytes := bucket.Get(key)
	var entry Entry

	if len(entryBytes) == 0 {
		entry = EntryFrom(ref)
	} else {
		if err := entry.Unmarshal(entryBytes); err != nil {
			return err
		}
		entry.Add(ref)
	}

	iBytes, err := entry.Marshal()
	if err != nil {
		return err
	}

	return bucket.Put(key, iBytes)
}

// removeRefFromBucket removes the reference from the bucket. It handles multiple reference on the same location
func removeRefFromBucket(bucket *bbolt.Bucket, key Key, ref Reference) error {
	entryBytes := bucket.Get(key)
	var entry Entry

	if len(entryBytes) == 0 {
		return nil
	}

	if err := entry.Unmarshal(entryBytes); err != nil {
		return err
	}
	entry.Delete(ref)

	if entry.Size() == 0 {
		return bucket.Delete(key)
	}

	iBytes, err := entry.Marshal()
	if err != nil {
		return err
	}

	return bucket.Put(key, iBytes)
}

func (i *index) IsMatch(query Query) float64 {
	hitcount := 0

	parts, err := i.sort(query)
	if err != nil {
		return 0.0
	}

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

func (i *index) sort(query Query) ([]QueryPart, error) {
	var sorted = make([]QueryPart, len(query.Parts()))

	hits := 0
	for _, qp := range query.Parts() {
		for j, ip := range i.indexParts {
			if ip.Name() == qp.Name() {
				if j >= len(sorted) {
					return nil, errors.New("invalid query part")
				}
				sorted[j] = qp
				hits++
			}
		}
	}

	if hits < len(query.Parts()) {
		// this could have been a type somewhere. If not caught it'll result in a lot of results when it shouldn't
		return nil, errors.New("unknown query part")
	}

	return sorted, nil
}

// Find documents given a search option.
func (i *index) Find(bucket *bbolt.Bucket, query Query) ([]Reference, error) {
	var err error

	cBucket := bucket.Bucket(i.BucketName())
	if cBucket == nil {
		return []Reference{}, err
	}

	// sort the parts of the Query to conform to the index key building order
	sortedQueryParts, err := i.sort(query)
	if err != nil {
		return nil, err
	}

	c := cBucket.Cursor()

	return findR(c, Key{}, sortedQueryParts)
}

func findR(cursor *bbolt.Cursor, sKey Key, parts []QueryPart) ([]Reference, error) {
	cPart := parts[0]
	seek, err := cPart.Seek()
	if err != nil {
		return nil, err
	}

	// to prevent duplicates
	refMap := map[string]bool{}
	var newRef = make([]Reference, 0)

	seek = ComposeKey(sKey, seek)
	condition := true
	for cKey, entry := cursor.Seek(seek); cKey != nil && bytes.HasPrefix(cKey, sKey) && condition; cKey, entry = cursor.Next() {
		// remove prefix (+1), Split and take first
		pf := cKey[len(sKey)+1:]
		if len(sKey) == 0 {
			pf = cKey
		}
		pfk := Key(pf)
		newp := pfk.Split()[0] // todo bounds check?

		condition, err = cPart.Condition(newp)
		if err != nil {
			return nil, err
		}
		if condition {
			var refs []Reference
			if len(parts) > 1 {
				nKey := ComposeKey(sKey, newp)
				refs, err = findR(cursor, nKey, parts[1:])

			} else {
				refs, err = entryToSlice(entry)
			}
			if err != nil {
				return nil, err
			}
			for _, r := range refs {
				if _, b := refMap[r.EncodeToString()]; !b {
					refMap[r.EncodeToString()] = true
					newRef = append(newRef, r)
				}
			}
		} else {
			eKey := ComposeKey(sKey, []byte{0xff, 0xff, 0xff, 0xff})
			_, _ = cursor.Seek(eKey)
		}
	}

	return newRef, nil
}

func entryToSlice(eBytes []byte) ([]Reference, error) {
	if eBytes == nil {
		return nil, nil
	}

	var entry Entry
	if err := entry.Unmarshal(eBytes); err != nil {
		return nil, err
	}

	return entry.Slice(), nil
}
