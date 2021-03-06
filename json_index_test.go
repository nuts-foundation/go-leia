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
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

// These tests are for index testing. It uses the json indexPart implementation

func TestIndex_AddJson(t *testing.T) {
	doc := Document(jsonExample)
	ref, _ := defaultReferenceCreator(doc)
	db := testDB(t)

	t.Run("ok - value added as key to document reference", func(t *testing.T) {
		i := NewIndex(t.Name(), jsonIndexPart{name: "key", jsonPath: "path.part"})

		db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		assertIndexed(t, db, i, []byte("value"), ref)
	})

	t.Run("ok - values added as key to document reference", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.parts"},
			jsonIndexPart{name: "key2", jsonPath: "path.part"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		assertIndexed(t, db, i, ComposeKey(Key("value1"), Key("value")), ref)
		assertIndexed(t, db, i, ComposeKey(Key("value3"), Key("value")), ref)
	})

	t.Run("ok - value added as key using recursion", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key2", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		k1, _ := toBytes(0.0)
		key := ComposeKey(Key("value"), k1)

		assertIndexed(t, db, i, key, ref)
	})

	t.Run("ok - multiple entries", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key2", jsonPath: "path.more.parts"},
		)
		doc2 := Document(jsonExample2)
		ref2, _ := defaultReferenceCreator(doc2)

		db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			i.Add(b, ref, doc)
			return i.Add(b, ref2, doc2)
		})

		k1, _ := toBytes(0.0)
		key := ComposeKey(Key("value"), k1)

		// check if both docs are indexed
		assertIndexed(t, db, i, key, ref)
		assertIndexed(t, db, i, key, ref2)
		assertIndexSize(t, db, i, 2)
	})

	t.Run("error - illegal document format", func(t *testing.T) {
		i := NewIndex(t.Name(), jsonIndexPart{name: "key", jsonPath: "path.parts"})

		err := db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, []byte("}"))
		})

		assert.Error(t, err)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, []byte("{}"))
		})

		assertIndexSize(t, db, i, 0)
	})
}

func TestIndex_Delete(t *testing.T) {
	doc := Document(jsonExample)
	ref, _ := defaultReferenceCreator(doc)
	db := testDB(t)

	t.Run("ok - value added and removed", func(t *testing.T) {
		i := NewIndex(t.Name(), jsonIndexPart{name: "key", jsonPath: "path.part"})

		db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			i.Add(b, ref, doc)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - value added and removed using recursion", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			i.Add(b, ref, doc)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("error - illegal document format", func(t *testing.T) {
		i := NewIndex(t.Name(), jsonIndexPart{name: "key", jsonPath: "path.parts"})

		err := db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			i.Add(b, ref, doc)
			return i.Delete(b, ref, []byte("}"))
		})

		assert.Error(t, err)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Delete(b, ref, []byte("{}"))
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - not indexed", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)

		db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - multiple entries", func(t *testing.T) {
		i := NewIndex(t.Name(),
			jsonIndexPart{name: "key", jsonPath: "path.part"},
			jsonIndexPart{name: "key", jsonPath: "path.more.parts"},
		)
		doc2 := Document(jsonExample2)
		ref2, _ := defaultReferenceCreator(doc2)

		err := db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			i.Add(b, ref, doc)
			i.Add(b, ref2, doc2)
			return i.Delete(b, ref, doc)
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
	doc := Document(jsonExample)
	ref, _ := defaultReferenceCreator(doc)
	doc2 := Document(jsonExample2)
	ref2, _ := defaultReferenceCreator(doc2)
	db := testDB(t)

	i := NewIndex(t.Name(),
		jsonIndexPart{name: "key", jsonPath: "path.part", tokenizer: WhiteSpaceTokenizer, transformer: ToLower},
		jsonIndexPart{name: "key2", jsonPath: "path.parts"},
		jsonIndexPart{name: "key3", jsonPath: "path.more.parts"},
	)

	db.Update(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		i.Add(b, ref, doc)
		return i.Add(b, ref2, doc2)
	})

	t.Run("ok - not found", func(t *testing.T) {
		q := New(Eq("key", "not_found"))
		found := false

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key []byte, value []byte) error {
				found = true
				return nil
			})
		})

		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("ok - exact match", func(t *testing.T) {
		q := New(Eq("key", "value")).And(Eq("key2", "value2")).And(Eq("key3", 1.0))
		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key []byte, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - match through transformer", func(t *testing.T) {
		q := New(Eq("key", "VALUE")).And(Eq("key2", "value2")).And(Eq("key3", 1.0))
		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key []byte, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - partial match", func(t *testing.T) {
		q := New(Eq("key", "value"))

		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key []byte, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		// it's a triple index where 4 matching trees exist
		assert.Equal(t, 4, count)
	})

	t.Run("error - wrong query", func(t *testing.T) {
		q := New(Eq("key3", "value"))

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key []byte, value []byte) error {
				return nil
			})
		})

		assert.Error(t, err)
	})
}
