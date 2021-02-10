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
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

// These tests are for index testing. It uses the json indexPart implementation

var json = `
{
	"path": {
		"part": "value",
		"parts": ["value1"],
		"more": [
			{
				"parts": 0.0
			}
		]
	}
}
`

var json2 = `
{
	"path": {
		"part": "value",
		"parts": ["value2"],
		"more": [
			{
				"parts": 0.0
			},
			{
				"parts": 1.0
			}
		]
	}
}
`

func TestIndex_Add(t *testing.T) {
	doc := Document(json)
	ref, _ := defaultReferenceCreator(doc)
	db := testDB(t)

	t.Run("ok - value added as key to document reference", func(t *testing.T) {
		i := NewIndex(t.Name(), jsonIndexPart{name: "key", jsonPath: "path.part"})

		db.Update(func(tx *bbolt.Tx) error {
			return i.Add(tx, ref, doc)
		})

		assertIndexed(t, db, i, []byte("value"), ref)
	})

	t.Run("ok - value added as key using recursion", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			return i.Add(tx, ref, doc)
		})

		k1, _ := toBytes(0.0)
		key := ComposeKey(Key("value"), k1)

		assertIndexed(t, db, i, key, ref)
	})

	t.Run("ok - multiple entries", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)
		doc2 := Document(json2)
		ref2, _ := defaultReferenceCreator(doc2)

		db.Update(func(tx *bbolt.Tx) error {
			i.Add(tx, ref, doc)
			return i.Add(tx, ref2, doc2)
		})

		k1, _ := toBytes(0.0)
		key := ComposeKey(Key("value"), k1)

		// check if both docs are indexed
		assertIndexed(t, db, i, key, ref)
		assertIndexed(t, db, i, key, ref2)
		assertSize(t, db, i, 2)
	})

	t.Run("error - illegal document format", func(t *testing.T) {
		i := NewIndex(t.Name(), jsonIndexPart{name: "key", jsonPath: "path.parts"})

		err := db.Update(func(tx *bbolt.Tx) error {
			return i.Add(tx, ref, []byte("}"))
		})

		assert.Error(t, err)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			return i.Add(tx, ref, []byte("{}"))
		})

		assertSize(t, db, i, 0)
	})
}

func TestIndex_Delete(t *testing.T) {
	doc := Document(json)
	ref, _ := defaultReferenceCreator(doc)
	db := testDB(t)

	t.Run("ok - value added and removed", func(t *testing.T) {
		i := NewIndex(t.Name(), jsonIndexPart{name: "key", jsonPath: "path.part"})

		db.Update(func(tx *bbolt.Tx) error {
			i.Add(tx, ref, doc)
			return i.Delete(tx, ref, doc)
		})

		assertSize(t, db, i, 0)
	})

	t.Run("ok - value added and removed using recursion", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			i.Add(tx, ref, doc)
			return i.Delete(tx, ref, doc)
		})

		assertSize(t, db, i, 0)
	})

	t.Run("error - illegal document format", func(t *testing.T) {
		i := NewIndex(t.Name(), jsonIndexPart{name: "key", jsonPath: "path.parts"})

		err := db.Update(func(tx *bbolt.Tx) error {
			return i.Delete(tx, ref, []byte("}"))
		})

		assert.Error(t, err)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			return i.Delete(tx, ref, []byte("{}"))
		})

		assertSize(t, db, i, 0)
	})

	t.Run("ok - not indexed", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			return i.Delete(tx, ref, doc)
		})

		assertSize(t, db, i, 0)
	})

	t.Run("ok - multiple entries", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)
		doc2 := Document(json2)
		ref2, _ := defaultReferenceCreator(doc2)

		err := db.Update(func(tx *bbolt.Tx) error {
			i.Add(tx, ref, doc)
			i.Add(tx, ref2, doc2)
			return i.Delete(tx, ref, doc)
		})

		if !assert.NoError(t, err) {
			return
		}

		k1, _ := toBytes(0.0)
		key := ComposeKey(Key("value"), k1)

		assertIndexed(t, db, i, key, ref2)
	})
}

func TestIndex_IsMatch(t *testing.T) {
	i := NewIndex(t.Name(),
		jsonIndexPart{name: "key", jsonPath: "path.part"},
		jsonIndexPart{name: "key2", jsonPath: "path.more.parts"},
	)

	t.Run("ok - exact match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key", "value")).
				And(Eq("key2", "value")))

		assert.Equal(t, 1.0, f)
	})

	t.Run("ok - exact match reverse ordering", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key2", "value")).
				And(Eq("key", "value")))

		assert.Equal(t, 1.0, f)
	})

	t.Run("ok - partial match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key", "value")))

		assert.Equal(t, 0.5, f)
	})

	t.Run("ok - no match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key3", "value")))

		assert.Equal(t, 0.0, f)
	})

	t.Run("ok - no match on second index only", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key2", "value")))

		assert.Equal(t, 0.0, f)
	})
}

func TestIndex_Find(t *testing.T) {
	doc := Document(json)
	ref, _ := defaultReferenceCreator(doc)
	doc2 := Document(json2)
	ref2, _ := defaultReferenceCreator(doc2)
	db := testDB(t)

	i := NewIndex(t.Name(),
		jsonIndexPart{name: "key", jsonPath: "path.part"},
		jsonIndexPart{name: "key2", jsonPath: "path.parts"},
		jsonIndexPart{name: "key3", jsonPath: "path.more.parts"},
	)

	db.Update(func(tx *bbolt.Tx) error {
		i.Add(tx, ref, doc)
		return i.Add(tx, ref2, doc2)
	})

	t.Run("ok - not found", func(t *testing.T) {
		q := New(Eq("key", "not_found"))

		var refs []Reference
		var err error
		db.View(func(tx *bbolt.Tx) error {
			refs, err = i.Find(tx, q)
			return err
		})

		assert.NoError(t, err)
		assert.Len(t, refs, 0)
	})

	t.Run("ok - exact match", func(t *testing.T) {
		q := New(Eq("key", "value")).And(Eq("key2", "value2")).And(Eq("key3", 1.0))

		var refs []Reference
		var err error
		db.View(func(tx *bbolt.Tx) error {
			refs, err = i.Find(tx, q)
			return err
		})

		assert.NoError(t, err)
		assert.Len(t, refs, 1)
	})

	t.Run("ok - partial match", func(t *testing.T) {
		q := New(Eq("key", "value"))

		var refs []Reference
		var err error
		db.View(func(tx *bbolt.Tx) error {
			refs, err = i.Find(tx, q)
			return err
		})

		assert.NoError(t, err)
		assert.Len(t, refs, 2)
	})

	t.Run("ok - nothing indexed", func(t *testing.T) {
		i := NewIndex(t.Name(), jsonIndexPart{name: "key", jsonPath: "path.part"})
		q := New(Eq("key", "value"))

		var refs []Reference
		var err error
		db.View(func(tx *bbolt.Tx) error {
			refs, err = i.Find(tx, q)
			return err
		})

		assert.NoError(t, err)
		assert.Len(t, refs, 0)
	})

	t.Run("error - wrong query", func(t *testing.T) {
		q := New(Eq("key3", "value"))

		err := db.View(func(tx *bbolt.Tx) error {
			_, err := i.Find(tx, q)
			return err
		})

		assert.Error(t, err)
	})
}

// assertIndexed checks if a key/value has been indexed
func assertIndexed(t *testing.T, db *bbolt.DB, i Index, key []byte, ref Reference) bool {
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(i.(*index).bucketName()))
		e := b.Get(key)

		refs, err := entryToSlice(e)

		if err != nil {
			return err
		}

		for _, r := range refs {
			if bytes.Compare(ref, r) == 0 {
				return nil
			}
		}

		return errors.New("ref not found")
	})

	return assert.NoError(t, err)
}

// assertEmpty check if an index is empty
func assertSize(t *testing.T, db *bbolt.DB, i Index, size int) bool {
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(i.(*index).bucketName()))
		assert.Equal(t, size, b.Stats().KeyN)
		return nil
	})

	return assert.NoError(t, err)
}