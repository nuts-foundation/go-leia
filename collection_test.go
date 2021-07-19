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
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var exampleDoc = Document{raw: []byte(jsonExample)}

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
		c.AddIndex(i)
		err := c.AddIndex(i)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, c.IndexList, 1)
	})

	t.Run("ok - new index adds refs", func(t *testing.T) {
		c := createCollection(db)
		err := c.Add([]Document{exampleDoc})
		assert.NoError(t, err)
		err = c.AddIndex(i)
		assert.NoError(t, err)

		assertIndexSize(t, db, i, 1)
		assertSize(t, db, GlobalCollection, 1)
	})

	t.Run("ok - adding existing index does nothing", func(t *testing.T) {
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{exampleDoc})

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
		c := createCollection(db)
		c.Add([]Document{exampleDoc})
		c.AddIndex(i)

		if !assert.NoError(t, c.DropIndex(i.Name())) {
			return
		}

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - dropping index leaves other indices at rest", func(t *testing.T) {
		i2 := NewIndex("other",
			NewFieldIndexer("path.part", AliasOption{Alias: "key"}),
		)
		c := createCollection(db)
		c.Add([]Document{exampleDoc})
		c.AddIndex(i)
		c.AddIndex(i2)

		if !assert.NoError(t, c.DropIndex(i.Name())) {
			return
		}

		assertIndexSize(t, db, i2, 1)
	})
}

func TestCollection_Add(t *testing.T) {
	db := testDB(t)

	errorRef := func(doc Document) (Reference, error) {
		return nil, errors.New("b00m!")
	}

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)
		err := c.Add([]Document{exampleDoc})
		if !assert.NoError(t, err) {
			return
		}

		assertSize(t, db, GlobalCollection, 1)
	})

	t.Run("error - refmake fails", func(t *testing.T) {
		c := createCollection(db)
		c.refMake = errorRef

		err := c.Add([]Document{exampleDoc})

		assert.Error(t, err)
	})
}

func TestCollection_Delete(t *testing.T) {
	i := testIndex(t)

	errorRef := func(doc Document) (Reference, error) {
		return nil, errors.New("b00m!")
	}

	t.Run("ok", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{exampleDoc})

		err := c.Delete(exampleDoc)
		if !assert.NoError(t, err) {
			return
		}

		assertIndexSize(t, db, i, 0)
		// the index sub-bucket counts as 1
		assertSize(t, db, c.Name, 1)
	})

	t.Run("ok - not added", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)

		err := c.Delete(exampleDoc)
		if !assert.NoError(t, err) {
			return
		}

		assertSize(t, db, c.Name, 0)
	})

	t.Run("error - refMake returns error", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)
		c.Add([]Document{exampleDoc})

		c.refMake = errorRef
		err := c.Delete(exampleDoc)

		assert.Error(t, err)
	})
}

func TestCollection_Find(t *testing.T) {
	db := testDB(t)
	i := testIndex(t)

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{exampleDoc})
		q := New(Eq("key", "value"))

		docs, err := c.Find(q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with ResultScan", func(t *testing.T) {
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{exampleDoc})
		q := New(Eq("key", "value")).And(Eq("non_indexed", "value"))

		docs, err := c.Find(q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with Full table scan", func(t *testing.T) {
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{exampleDoc})
		q := New(Eq("non_indexed", "value"))

		docs, err := c.Find(q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with ResultScan and range query", func(t *testing.T) {
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{exampleDoc})
		q := New(Eq("key", "value")).And(Range("non_indexed", "v", "value1"))

		docs, err := c.Find(q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with ResultScan, range query not found", func(t *testing.T) {
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{exampleDoc})
		q := New(Eq("key", "value")).And(Range("non_indexed", "value1", "value2"))

		docs, err := c.Find(q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 0)
	})

	t.Run("ok - no docs", func(t *testing.T) {
		db := testDB(t)
		c := createCollection(db)
		c.AddIndex(i)
		q := New(Eq("key", "value"))

		docs, err := c.Find(q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 0)
	})

	t.Run("error - incorrect query", func(t *testing.T) {
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{exampleDoc})
		q := New(Eq("key", struct{}{}))

		_, err := c.Find(q)

		assert.Error(t, err)
	})

	t.Run("error - nil query", func(t *testing.T) {
		c := createCollection(db)
		c.AddIndex(i)
		c.Add([]Document{exampleDoc})

		_, err := c.Find(nil)

		assert.Error(t, err)
	})
}

func TestCollection_Iterate(t *testing.T) {
	db := testDB(t)
	i := testIndex(t)
	c := createCollection(db)
	c.AddIndex(i)
	c.Add([]Document{exampleDoc})
	q := New(Eq("key", "value"))

	t.Run("ok - count fn", func(t *testing.T) {
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

	t.Run("error", func(t *testing.T) {
		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key []byte, value []byte) error {
				return errors.New("b00m!")
			})
		})

		assert.Error(t, err)
	})
}

func TestCollection_IndexIterate(t *testing.T) {
	db := testDB(t)
	i := testIndex(t)
	c := createCollection(db)
	c.AddIndex(i)
	c.Add([]Document{exampleDoc})
	q := New(Eq("key", "value"))

	t.Run("ok - count fn", func(t *testing.T) {
		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			return c.IndexIterate(q, func(key []byte, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("error", func(t *testing.T) {
		err := db.View(func(tx *bbolt.Tx) error {
			return c.IndexIterate(q, func(key []byte, value []byte) error {
				return errors.New("b00m!")
			})
		})

		assert.Error(t, err)
	})
}

func TestCollection_Reference(t *testing.T) {
	db := testDB(t)

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)

		ref, err := c.Reference(exampleDoc)

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, "d29cb76cae7662a142e36c85eb39f4caa7fa593f", ref.EncodeToString())
	})
}

func TestCollection_Get(t *testing.T) {
	db := testDB(t)

	t.Run("ok", func(t *testing.T) {
		c := createCollection(db)
		ref, _ := defaultReferenceCreator(exampleDoc)
		c.Add([]Document{exampleDoc})

		d, err := c.Get(ref)

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, exampleDoc, *d)
	})

	t.Run("error - not found", func(t *testing.T) {
		c := createCollection(db)

		d, err := c.Get([]byte("test"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Nil(t, d)
	})
}

func testIndex(t *testing.T) Index {
	return NewIndex(t.Name(),
		NewFieldIndexer("path.part", AliasOption{Alias: "key"}),
	)
}

func createCollection(db *bbolt.DB) collection {
	gCollection := collection{
		Name:      GlobalCollection,
		db:        db,
		IndexList: []Index{},
		refMake:   defaultReferenceCreator,
	}

	return collection{
		Name:             "test",
		db:               db,
		globalCollection: &gCollection,
		IndexList:        []Index{},
		refMake:          defaultReferenceCreator,
	}
}
