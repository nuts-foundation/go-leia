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
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var valueAsScalar = ScalarMustParse("value")

func TestNewIndex(t *testing.T) {
	_, c := testCollection(t)
	i := c.NewIndex("name")

	assert.Equal(t, "name", i.Name())
	assert.Len(t, i.(*index).indexParts, 0)
}

func TestIndex_AddJson(t *testing.T) {
	doc := []byte(jsonExample)
	ref := defaultReferenceCreator(doc)
	doc2 := []byte(jsonExample2)
	ref2 := defaultReferenceCreator(doc2)
	db, c := testCollection(t)

	t.Run("ok - value added as key to document reference", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer("path.part", AliasOption("key")))

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		assertIndexed(t, db, i, []byte("value"), ref)
	})

	t.Run("ok - values added as key to document reference", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.parts", AliasOption("key")),
			NewFieldIndexer("path.part", AliasOption("key2")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		assertIndexed(t, db, i, ComposeKey(Key("value1"), Key("value")), ref)
		assertIndexed(t, db, i, ComposeKey(Key("value3"), Key("value")), ref)
	})

	t.Run("ok - value added as key using recursion", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.part", AliasOption("key")),
			NewFieldIndexer("path.more.#.parts", AliasOption("key2")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		k1, _ := toBytes(0.0)
		key := ComposeKey(Key("value"), k1)

		assertIndexed(t, db, i, key, ref)
	})

	t.Run("ok - multiple entries", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.part", AliasOption("key")),
			NewFieldIndexer("path.more.#.parts", AliasOption("key2")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Add(b, ref2, doc2)
		})

		k1, _ := toBytes(0.0)
		key := ComposeKey(Key("value"), k1)

		// check if both docs are indexed
		assertIndexed(t, db, i, key, ref)
		assertIndexed(t, db, i, key, ref2)
		assertIndexSize(t, db, i, 3)
	})

	t.Run("error - illegal document format", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer("path.parts", AliasOption("key")))

		err := db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, []byte("}"))
		})

		assert.Error(t, err)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.part", AliasOption("key")),
			NewFieldIndexer("path.more.parts", AliasOption("key")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, []byte("{}"))
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - value added with nil index value", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.part", AliasOption("key")),
			NewFieldIndexer("path.unknown", AliasOption("key2")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			_ = i.Add(testBucket(t, tx), ref, doc)
			return i.Add(testBucket(t, tx), ref2, doc2)
		})

		key := ComposeKey(Key("value"), []byte{})

		assertIndexed(t, db, i, key, ref)
		assertIndexed(t, db, i, key, ref2)
		assertIndexSize(t, db, i, 2)
	})
}

func TestIndex_Delete(t *testing.T) {
	doc := []byte(jsonExample)
	ref := defaultReferenceCreator(doc)
	db, c := testCollection(t)

	t.Run("ok - value added and removed", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer("path.part", AliasOption("key")))

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - value added and removed using recursion", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.part", AliasOption("key")),
			NewFieldIndexer("path.more.#.parts", AliasOption("key2")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("error - illegal document format", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer("path.parts", AliasOption("key")))

		err := db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Delete(b, ref, []byte("}"))
		})

		assert.Error(t, err)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.part", AliasOption("key")),
			NewFieldIndexer("path.more.#.parts", AliasOption("key2")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Delete(b, ref, []byte("{}"))
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - not indexed", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.part", AliasOption("key")),
			NewFieldIndexer("path.more.#.parts", AliasOption("key2")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - multiple entries", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.part", AliasOption("key")),
			NewFieldIndexer("path.more.#.parts", AliasOption("key2")),
		)
		doc2 := []byte(jsonExample2)
		ref2 := defaultReferenceCreator(doc2)

		err := db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			_ = i.Add(b, ref2, doc2)
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
	_, c := testCollection(t)
	i := c.NewIndex(t.Name(),
		NewFieldIndexer("path.part", AliasOption("key")),
		NewFieldIndexer("path.more.#.parts", AliasOption("key2")),
	)

	t.Run("ok - exact match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key", valueAsScalar)).
				And(Eq("key2", valueAsScalar)))

		assert.Equal(t, 1.0, f)
	})

	t.Run("ok - exact match reverse ordering", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key2", valueAsScalar)).
				And(Eq("key", valueAsScalar)))

		assert.Equal(t, 1.0, f)
	})

	t.Run("ok - partial match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key", valueAsScalar)))

		assert.Equal(t, 0.5, f)
	})

	t.Run("ok - no match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key3", valueAsScalar)))

		assert.Equal(t, 0.0, f)
	})

	t.Run("ok - no match on second index only", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq("key2", valueAsScalar)))

		assert.Equal(t, 0.0, f)
	})
}

func TestIndex_Find(t *testing.T) {
	doc := []byte(jsonExample)
	ref := defaultReferenceCreator(doc)
	doc2 := []byte(jsonExample2)
	ref2 := defaultReferenceCreator(doc2)
	db, c := testCollection(t)

	i := c.NewIndex(t.Name(),
		NewFieldIndexer("path.part", AliasOption("key"), TokenizerOption(WhiteSpaceTokenizer), TransformerOption(ToLower)),
		NewFieldIndexer("path.parts", AliasOption("key2")),
		NewFieldIndexer("path.more.#.parts", AliasOption("key3")),
	)

	_ = db.Update(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		_ = i.Add(b, ref, doc)
		return i.Add(b, ref2, doc2)
	})

	t.Run("ok - not found", func(t *testing.T) {
		q := New(Eq("key", ScalarMustParse("not_found")))
		found := false

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				found = true
				return nil
			})
		})

		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("ok - exact match", func(t *testing.T) {
		q := New(Eq("key", valueAsScalar)).And(
			Eq("key2", ScalarMustParse("value2"))).And(
			Eq("key3", ScalarMustParse(1.0)))
		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - match through transformer", func(t *testing.T) {
		q := New(Eq("key", ScalarMustParse("VALUE"))).And(
			Eq("key2", ScalarMustParse("value2"))).And(
			Eq("key3", ScalarMustParse(1.0)))
		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - partial match", func(t *testing.T) {
		q := New(Eq("key", valueAsScalar))

		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		// it's a triple index where 4 matching trees exist
		assert.Equal(t, 4, count)
	})

	t.Run("ok - match with nil values at multiple levels", func(t *testing.T) {
		db, c := testCollection(t)

		i := c.NewIndex(t.Name(),
			NewFieldIndexer("path.part", AliasOption("key"), TokenizerOption(WhiteSpaceTokenizer), TransformerOption(ToLower)),
			NewFieldIndexer("path.unknown", AliasOption("key2")),
			NewFieldIndexer("path.unknown2", AliasOption("key3")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Add(b, ref2, doc2)
		})

		q := New(Eq("key", valueAsScalar))

		count := 0

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		// it's a triple index where 4 matching trees exist
		assert.Equal(t, 2, count)
	})

	t.Run("error - wrong query", func(t *testing.T) {
		q := New(Eq("key3", valueAsScalar))

		err := db.View(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Iterate(b, q, func(key Reference, value []byte) error {
				return nil
			})
		})

		assert.Error(t, err)
	})
}

func TestIndex_findR(t *testing.T) {
	doc := []byte(jsonExample)
	ref := defaultReferenceCreator(doc)
	db, c := testCollection(t)

	i := c.NewIndex(t.Name(),
		NewFieldIndexer("path.part", AliasOption("key"), TokenizerOption(WhiteSpaceTokenizer), TransformerOption(ToLower)),
	).(*index)

	_ = db.Update(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		return i.Add(b, ref, doc)
	})

	q := New(Eq("key", valueAsScalar))
	matchers := i.matchers(q.Parts())
	var found bool
	foundFunc := func(key Reference, value []byte) error {
		found = true
		return nil
	}

	t.Run("match when cursor at beginning", func(t *testing.T) {
		found = false

		// by passing the value to be found as latest cursor value, it should skip over the results
		err := db.View(func(tx *bbolt.Tx) error {
			cursor := testBucket(t, tx).Bucket(i.BucketName()).Cursor()
			return findR(cursor, []byte{}, matchers, foundFunc, []byte{})
		})

		assert.NoError(t, err)
		assert.True(t, found)
	})

	t.Run("skip match when cursor already further", func(t *testing.T) {
		found = false

		// by passing the value to be found as latest cursor value, it should skip over the results
		err := db.View(func(tx *bbolt.Tx) error {
			cursor := testBucket(t, tx).Bucket(i.BucketName()).Cursor()
			return findR(cursor, []byte{}, matchers, foundFunc, valueAsScalar.Bytes())
		})

		assert.NoError(t, err)
		assert.False(t, found)
	})
}

func TestIndex_addRefToBucket(t *testing.T) {
	t.Run("adding more than 16 entries", func(t *testing.T) {
		db := testDB(t)

		err := db.Update(func(tx *bbolt.Tx) error {
			bucket := testBucket(t, tx)

			for i := uint32(0); i < 16; i++ {
				iBytes, _ := toBytes(i)
				if err := addRefToBucket(bucket, []byte("key"), iBytes); err != nil {
					return err
				}
			}

			return nil
		})

		assert.NoError(t, err)

		// stats are not updated until after commit
		_ = db.View(func(tx *bbolt.Tx) error {
			bucket := testBucket(t, tx)
			b := bucket.Bucket([]byte("key"))

			assert.NotNil(t, b)
			assert.Equal(t, 16, b.Stats().KeyN)

			return nil
		})
	})
}

func TestIndex_Sort(t *testing.T) {
	_, c := testCollection(t)
	i := c.NewIndex(t.Name(),
		NewFieldIndexer("path.part", AliasOption("key")),
		NewFieldIndexer("path.more.#.parts", AliasOption("key2")),
	)

	t.Run("returns correct order when given in reverse", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq("key2", valueAsScalar)).
				And(Eq("key", valueAsScalar)), false)

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, "key", sorted[0].Name())
		assert.Equal(t, "key2", sorted[1].Name())
	})

	t.Run("returns correct order when given in correct order", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq("key", valueAsScalar)).
				And(Eq("key2", valueAsScalar)), false)

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, "key", sorted[0].Name())
		assert.Equal(t, "key2", sorted[1].Name())
	})

	t.Run("does not include any keys when primary key is missing", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq("key2", valueAsScalar)), false)

		assert.Len(t, sorted, 0)
	})

	t.Run("includes all keys when includeMissing option is given", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq("key3", valueAsScalar)).
				And(Eq("key2", valueAsScalar)), true)

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, "key3", sorted[0].Name())
		assert.Equal(t, "key2", sorted[1].Name())
	})

	t.Run("includes additional keys when includeMissing option is given", func(t *testing.T) {
		sorted := i.Sort(
			New(Eq("key3", valueAsScalar)).
				And(Eq("key", valueAsScalar)), true)

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, "key", sorted[0].Name())
		assert.Equal(t, "key3", sorted[1].Name())
	})
}

func TestIndex_QueryPartsOutsideIndex(t *testing.T) {
	_, c := testCollection(t)
	i := c.NewIndex(t.Name(),
		NewFieldIndexer("path.part", AliasOption("key")),
		NewFieldIndexer("path.more.#.parts", AliasOption("key2")),
	)

	t.Run("returns empty list when all parts in index", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq("key2", valueAsScalar)).
				And(Eq("key", valueAsScalar)))

		assert.Len(t, additional, 0)
	})

	t.Run("returns all parts when none match index", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq("key2", valueAsScalar)))

		assert.Len(t, additional, 1)
	})

	t.Run("returns correct params on partial index match", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq("key3", valueAsScalar)).
				And(Eq("key", valueAsScalar)))

		if !assert.Len(t, additional, 1) {
			return
		}
		assert.Equal(t, "key3", additional[0].Name())
	})
}

func TestIndex_Keys(t *testing.T) {
	json := `
{
	"path": {
		"part": "value",
		"parts": ["value1", "value2"],
		"more": [{
			"parts": 0.0
		}]
	}
}
`
	document := []byte(json)

	t.Run("ok - sub object", func(t *testing.T) {
		_, _, i := testIndex(t)
		ip := NewFieldIndexer("path.part")
		keys, err := i.Keys(ip, document)

		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 1) {
			return
		}

		assert.Equal(t, "value", keys[0].value)
	})

	t.Run("ok - sub sub object", func(t *testing.T) {
		_, _, i := testIndex(t)
		ip := NewFieldIndexer("path.more.#.parts")
		keys, err := i.Keys(ip, document)

		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 1) {
			return
		}

		bits := binary.BigEndian.Uint64(keys[0].Bytes())
		fl := math.Float64frombits(bits)

		assert.Equal(t, 0.0, fl)
	})

	t.Run("ok - list", func(t *testing.T) {
		_, _, i := testIndex(t)
		ip := NewFieldIndexer("path.parts")
		keys, err := i.Keys(ip, document)

		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 2) {
			return
		}

		assert.Equal(t, "value1", keys[0].value)
		assert.Equal(t, "value2", keys[1].value)
	})

	t.Run("ok - no match", func(t *testing.T) {
		_, _, i := testIndex(t)
		ip := NewFieldIndexer("path.party")
		keys, err := i.Keys(ip, document)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, keys, 0)
	})

	t.Run("error - incorrect document", func(t *testing.T) {
		_, _, i := testIndex(t)
		ip := NewFieldIndexer("path.part")

		_, err := i.Keys(ip, []byte("}"))

		assert.Error(t, err)
	})
}
