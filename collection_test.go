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

func TestCollection_AddIndex(t *testing.T) {
	db := testDB(t)
	i := testIndex(t)

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)
		err := c.AddIndex(i)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, c.IndexList, 1)
	})

	t.Run("ok - duplicate", func(t *testing.T) {
		c := createCollection(db)
		err := c.AddIndex(i)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, c.IndexList, 1)
	})

	t.Run("ok - new index adds refs", func(t *testing.T) {
		doc := Document(json)
		c := createCollection(db)
		c.Add([]Document{doc})
		c.AddIndex(i)

		assertIndexSize(t, db, i, 1)
	})

	t.Run("ok - adding existing index does nothing", func(t *testing.T) {
		doc := Document(json)
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{doc})

		assertIndexSize(t, db, i, 1)

		c2 := createCollection(db)
		c2.AddIndex(i)

		assertIndexSize(t, db, i, 1)
	})
}

func TestCollection_DropIndex(t *testing.T) {
	db := testDB(t)
	i := testIndex(t)

	t.Run("ok - dropping index removes refs", func(t *testing.T) {
		doc := Document(json)
		c := createCollection(db)
		c.Add([]Document{doc})
		c.AddIndex(i)

		if !assert.NoError(t, c.DropIndex(i.Name())) {
			return
		}

		assertIndexSize(t, db, i, 0)
	})
}

func TestCollection_Add(t *testing.T) {
	db := testDB(t)

	t.Run("ok", func(t *testing.T) {
		doc := Document(json)
		c := createCollection(db)
		err := c.Add([]Document{doc})
		if !assert.NoError(t, err) {
			return
		}

		assertSize(t, db, c.Name, 1)
	})
}

func TestCollection_Delete(t *testing.T) {
	db := testDB(t)
	i := testIndex(t)

	t.Run("ok", func(t *testing.T) {
		doc := Document(json)
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{doc})

		err := c.Delete(doc)
		if !assert.NoError(t, err) {
			return
		}

		assertIndexSize(t, db, i, 0)
		// the index sub-bucket counts as 1
		assertSize(t, db, c.Name, 1)
	})
}

func TestCollection_Find(t *testing.T) {
	db := testDB(t)
	i := testIndex(t)

	t.Run("ok", func(t *testing.T) {
		doc := Document(json)
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{doc})
		q := New(Eq("key","value"))

		docs, err := c.Find(q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("error - incorrect query", func(t *testing.T) {
		doc := Document(json)
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{doc})
		q := New(Eq("key",struct{}{}))

		_, err := c.Find(q)

		assert.Error(t, err)
	})

	t.Run("error - no index", func(t *testing.T) {
		c := createCollection(db)
		q := New(Eq("key","value"))

		_, err := c.Find(q)

		assert.Error(t, err)
	})
}

func TestCollection_Reference(t *testing.T) {
	db := testDB(t)

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)
		doc := Document(json)

		ref, err := c.Reference(doc)

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, "e7b9d2c3f90ae1f37b5e1ebbc8092e700fa1483c14643da8f4cd05de2c15c67d", ref.EncodeToString())
	})
}

func testIndex(t *testing.T) Index {
	return NewIndex(t.Name(),
		jsonIndexPart{name: "key", jsonPath: "path.part"},
	)
}

func createCollection(db *bbolt.DB) collection {
	return collection {
		Name:      "test",
		db:        db,
		IndexList: []Index{},
		refMake:   defaultReferenceCreator,
	}
}
