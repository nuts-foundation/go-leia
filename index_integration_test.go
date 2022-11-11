/*
 * go-leia
 * Copyright (C) 2022 Nuts community
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

var doc1 = []byte(`
{
	"key1": "1",
	"key2": "23"
}
`)

var doc2 = []byte(`
{
	"key1": "12",
	"key2": "3"
}
`)

var doc3 = []byte(`
{
	"key1": "0"
}
`)

// TestIndex_CursorDynamics contains a set of tests to see if all relevant documents are returned
func TestIndex_CursorDynamics(t *testing.T) {
	ref1 := defaultReferenceCreator(doc1)
	ref2 := defaultReferenceCreator(doc2)
	ref3 := defaultReferenceCreator(doc3)
	key1 := NewJSONPath("key1")
	key2 := NewJSONPath("key2")
	db, c := testCollection(t)

	i := c.NewIndex(t.Name(),
		NewFieldIndexer(key1),
		NewFieldIndexer(key2),
	)

	_ = db.Update(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		_ = i.Add(b, ref1, doc1)
		_ = i.Add(b, ref2, doc2)
		return i.Add(b, ref3, doc3)
	})

	t.Run("2 docs found on single prefix key", func(t *testing.T) {
		q := New(Prefix(key1, MustParseScalar("1")))
		found := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				found++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, found)
	})

	t.Run("2 docs found on single prefix key using duplicate key", func(t *testing.T) {
		q := New(Prefix(key1, MustParseScalar("1"))).And(
			Prefix(key1, MustParseScalar("1")))
		found := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				found++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, found)
	})

	t.Run("2 docs found on prefix key and notNil", func(t *testing.T) {
		q := New(Prefix(key1, MustParseScalar("1"))).And(NotNil(key2))
		found := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				found++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, found)
	})

	t.Run("2 docs found on empty prefix key and notNil", func(t *testing.T) {
		// the first hit for the cursor on the empty prefix is a hit where the second matcher fails (notNil)
		// A bug prevented continuing evaluation by breaking instead of advancing the cursor and continuing
		q := New(Prefix(key1, MustParseScalar(""))).And(NotNil(key2))
		found := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				found++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, found)
	})
}
