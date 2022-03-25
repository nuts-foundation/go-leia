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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piprate/json-gold/ld"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var exampleDoc = []byte(jsonExample)

func TestCollection_AddIndex(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		_, c, i := testIndex(t)
		err := c.AddIndex(i)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, c.indexList, 1)
	})

	t.Run("ok - duplicate", func(t *testing.T) {
		_, c, i := testIndex(t)
		_ = c.AddIndex(i)
		err := c.AddIndex(i)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, c.indexList, 1)
	})

	t.Run("ok - new index adds refs", func(t *testing.T) {
		db, c, i := testIndex(t)
		err := c.Add([]Document{exampleDoc})
		assert.NoError(t, err)
		err = c.AddIndex(i)
		assert.NoError(t, err)

		assertIndexSize(t, db, i, 1)
		assertSize(t, db, documentCollection, 1)
	})

	t.Run("ok - adding existing index does nothing", func(t *testing.T) {
		db, c, i := testIndex(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{exampleDoc})

		assertIndexSize(t, db, i, 1)

		_ = c.AddIndex(i)

		assertIndexSize(t, db, i, 1)
	})
}

func TestCollection_DropIndex(t *testing.T) {
	t.Run("ok - dropping index removes refs", func(t *testing.T) {
		db, c, i := testIndex(t)
		_ = c.Add([]Document{exampleDoc})
		_ = c.AddIndex(i)

		if !assert.NoError(t, c.DropIndex(i.Name())) {
			return
		}

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - dropping index leaves other indices at rest", func(t *testing.T) {
		db, c, i := testIndex(t)
		i2 := c.NewIndex("other",
			NewFieldIndexer(NewJSONPath("path.part")),
		)
		_ = c.Add([]Document{exampleDoc})
		_ = c.AddIndex(i)
		_ = c.AddIndex(i2)

		if !assert.NoError(t, c.DropIndex(i.Name())) {
			return
		}

		assertIndexSize(t, db, i2, 1)
	})
}

func TestCollection_Add(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		db, c := testCollection(t)
		err := c.Add([]Document{exampleDoc})
		if !assert.NoError(t, err) {
			return
		}

		assertSize(t, db, documentCollection, 1)
	})
}

func TestCollection_Delete(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		db, c, i := testIndex(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{exampleDoc})

		err := c.Delete(exampleDoc)
		if !assert.NoError(t, err) {
			return
		}

		assertIndexSize(t, db, i, 0)
		assertSize(t, db, documentCollection, 0)
	})

	t.Run("ok - not added", func(t *testing.T) {
		db, c, _ := testIndex(t)

		err := c.Delete(exampleDoc)
		if !assert.NoError(t, err) {
			return
		}

		assertSize(t, db, documentCollection, 0)
	})
}

func TestCollection_Find(t *testing.T) {
	key := NewJSONPath("path.part")
	nonIndexed := NewJSONPath("non_indexed")

	t.Run("ok", func(t *testing.T) {
		_, c, i := testIndex(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{exampleDoc})
		q := New(Eq(key, MustParseScalar("value")))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with ResultScan", func(t *testing.T) {
		_, c, i := testIndex(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{exampleDoc})
		q := New(Eq(key, MustParseScalar("value"))).And(Eq(nonIndexed, MustParseScalar("value")))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with Full table scan", func(t *testing.T) {
		_, c, i := testIndex(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{exampleDoc})
		q := New(Eq(nonIndexed, MustParseScalar("value")))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with ResultScan and range query", func(t *testing.T) {
		_, c, i := testIndex(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{exampleDoc})
		q := New(Eq(key, MustParseScalar("value"))).And(Range(nonIndexed, MustParseScalar("v"), MustParseScalar("value1")))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 1)
	})

	t.Run("ok - with ResultScan, range query not found", func(t *testing.T) {
		_, c, i := testIndex(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{exampleDoc})
		q := New(Eq(key, MustParseScalar("value"))).And(
			Range(nonIndexed, MustParseScalar("value1"), MustParseScalar("value2")))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 0)
	})

	t.Run("ok - no docs", func(t *testing.T) {
		_, c, i := testIndex(t)
		_ = c.AddIndex(i)
		q := New(Eq(key, MustParseScalar("value")))

		docs, err := c.Find(context.TODO(), q)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, docs, 0)
	})

	t.Run("error - ctx cancelled", func(t *testing.T) {
		_, c, i := testIndex(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{exampleDoc})
		q := New(Eq(key, MustParseScalar("value")))
		ctx, cancelFn := context.WithCancel(context.Background())

		cancelFn()
		_, err := c.Find(ctx, q)

		if !assert.Error(t, err) {
			return
		}

		assert.Equal(t, context.Canceled, err)
	})

	t.Run("error - deadline exceeded", func(t *testing.T) {
		_, c, i := testIndex(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{exampleDoc})
		q := New(Eq(key, MustParseScalar("value")))
		ctx, _ := context.WithTimeout(context.Background(), time.Nanosecond)

		_, err := c.Find(ctx, q)

		if !assert.Error(t, err) {
			return
		}

		assert.Equal(t, context.DeadlineExceeded, err)
	})
}

func TestCollection_Iterate(t *testing.T) {
	key := NewJSONPath("path.part")

	_, c, i := testIndex(t)
	_ = c.AddIndex(i)
	_ = c.Add([]Document{exampleDoc})
	q := New(Eq(key, MustParseScalar("value")))

	t.Run("ok - count fn", func(t *testing.T) {
		count := 0

		err := c.Iterate(q, func(key Reference, value []byte) error {
			count++
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - document indexed multiple times, query should un double", func(t *testing.T) {
		doc := []byte(jsonExample)
		doc2 := []byte(jsonExample2)
		count := 0

		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewJSONPath("path.part")),
			NewFieldIndexer(NewJSONPath("path.more.#.parts")),
		)

		_, c := testCollection(t)
		_ = c.AddIndex(i)
		_ = c.Add([]Document{doc, doc2})

		err := c.Iterate(q, func(key Reference, value []byte) error {
			count++
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("error", func(t *testing.T) {
		err := c.Iterate(q, func(key Reference, value []byte) error {
			return errors.New("b00m")
		})

		assert.Error(t, err)
	})
}

func TestCollection_IndexIterate(t *testing.T) {
	db, c, i := testIndex(t)
	_ = c.AddIndex(i)
	_ = c.Add([]Document{exampleDoc})
	q := New(Eq(NewJSONPath("path.part"), MustParseScalar("value")))

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
				return errors.New("b00m")
			})
		})

		assert.Error(t, err)
	})
}

func TestCollection_Reference(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		_, c := testCollection(t)

		ref := c.Reference(exampleDoc)

		assert.Equal(t, "d29cb76cae7662a142e36c85eb39f4caa7fa593f", ref.EncodeToString())
	})
}

func TestCollection_Get(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		_, c := testCollection(t)
		ref := defaultReferenceCreator(exampleDoc)
		if err := c.Add([]Document{exampleDoc}); err != nil {
			t.Fatal(err)
		}

		d, err := c.Get(ref)

		if !assert.NoError(t, err) {
			return
		}

		if assert.NotNil(t, d) {
			assert.Equal(t, Document(exampleDoc), d)
		}
	})

	t.Run("error - not found", func(t *testing.T) {
		_, c := testCollection(t)

		d, err := c.Get([]byte("test"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Nil(t, d)
	})
}

func TestCollection_JSONPathValueCollector(t *testing.T) {
	json := []byte(`
{
	"id": 1,
	"path": "test",
	"colors": ["blue", "orange"],
	"items" : [
		{
			"type": "car",
			"count": 2
		},
		{
			"type": "bike",
			"count": 5
		}
	],
	"animals": [
		{
			"nesting": {
				"type": "bird"
			}
		}
	]
}
`)

	c := collection{
		valueCollector: JSONPathValueCollector,
	}

	t.Run("ok - find a single float value", func(t *testing.T) {
		values, err := c.ValuesAtPath(json, NewJSONPath("id"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 1)
		assert.Equal(t, 1.0, values[0].value())
	})

	t.Run("ok - find a single string value", func(t *testing.T) {
		values, err := c.ValuesAtPath(json, NewJSONPath("path"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 1)
		assert.Equal(t, "test", values[0].value())
	})

	t.Run("ok - find a list of values", func(t *testing.T) {
		values, err := c.ValuesAtPath(json, NewJSONPath("colors"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 2)
		assert.Equal(t, "blue", values[0].value())
		assert.Equal(t, "orange", values[1].value())
	})

	t.Run("ok - find a list of values from a sublist", func(t *testing.T) {
		values, err := c.ValuesAtPath(json, NewJSONPath("items.#.type"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 2)
		assert.Equal(t, "car", values[0].value())
		assert.Equal(t, "bike", values[1].value())
	})

	t.Run("ok - values at an unknown path", func(t *testing.T) {
		values, err := c.ValuesAtPath(json, NewJSONPath("unknown"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 0)
	})

	t.Run("error - invalid json", func(t *testing.T) {
		_, err := c.ValuesAtPath([]byte("}"), NewJSONPath("id"))

		assert.Equal(t, ErrInvalidJSON, err)
	})

	t.Run("error - indexing an object", func(t *testing.T) {
		_, err := c.ValuesAtPath(json, NewJSONPath("animals.#.nesting"))

		assert.EqualError(t, err, "type at path not supported for indexing: {\n\t\t\t\t\"type\": \"bird\"\n\t\t\t}")
	})
}

func TestCollection_JSONLDValueCollector(t *testing.T) {

	document := Document(jsonLDExample)

	c := collection{
		documentProcessor: ld.NewJsonLdProcessor(),
		valueCollector:    JSONLDValueCollector,
	}

	t.Run("ok - find a single string value", func(t *testing.T) {
		values, err := c.ValuesAtPath(document, NewTermPath("http://example.com/name"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 1)
		assert.Equal(t, "Jane Doe", values[0].value())
	})

	t.Run("ok - find a single nested string value", func(t *testing.T) {
		values, err := c.ValuesAtPath(document, NewTermPath("http://example.com/children", "http://example.com/name"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 1)
		assert.Equal(t, "John Doe", values[0].value())
	})
}

func testIndex(t *testing.T) (*bbolt.DB, *collection, Index) {
	db := testDB(t)
	c := testCollectionWithDB(db)

	return db, c, c.NewIndex(t.Name(),
		NewFieldIndexer(NewJSONPath("path.part")),
	)
}

func testCollection(t *testing.T) (*bbolt.DB, *collection) {
	db := testDB(t)
	return db, testCollectionWithDB(db)
}

func testCollectionWithDB(db *bbolt.DB) *collection {
	return &collection{
		name:           "test",
		db:             db,
		indexList:      []Index{},
		refMake:        defaultReferenceCreator,
		valueCollector: JSONPathValueCollector,
	}
}
