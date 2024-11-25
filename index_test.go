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

var valueAsScalar = MustParseScalar("value")
var valueAsScalar2 = MustParseScalar("value2")

func TestNewIndex(t *testing.T) {
	_, c := testCollection(t)
	i := c.NewIndex("path")

	assert.Equal(t, "path", i.Name())
	assert.Len(t, i.(*index).indexParts, 0)
}

func TestIndex_AddJson(t *testing.T) {
	doc := []byte(jsonExample)
	ref := defaultReferenceCreator(doc)
	doc2 := []byte(jsonExample2)
	ref2 := defaultReferenceCreator(doc2)
	db, c := testCollection(t)

	t.Run("ok - value added as key to document reference", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer(NewJSONPath("path.part")))

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		assertIndexed(t, db, i, []byte("value"), ref)
	})

	t.Run("ok - values added as key to document reference", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewJSONPath("path.parts")),
			NewFieldIndexer(NewJSONPath("path.part")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, doc)
		})

		assertIndexed(t, db, i, ComposeKey(Key("value1"), Key("value")), ref)
		assertIndexed(t, db, i, ComposeKey(Key("value3"), Key("value")), ref)
	})

	t.Run("ok - value added as key using recursion", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewJSONPath("path.part")),
			NewFieldIndexer(NewJSONPath("path.more.#.parts")),
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
			NewFieldIndexer(NewJSONPath("path.part")),
			NewFieldIndexer(NewJSONPath("path.more.#.parts")),
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
		i := c.NewIndex(t.Name(), NewFieldIndexer(NewJSONPath("path.parts")))

		err := db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, []byte("}"))
		})

		assert.Error(t, err)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewJSONPath("path.part")),
			NewFieldIndexer(NewJSONPath("path.more.parts")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			return i.Add(testBucket(t, tx), ref, []byte("{}"))
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - value added with nil index value", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewJSONPath("path.part")),
			NewFieldIndexer(NewJSONPath("path.unknown")),
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
		i := c.NewIndex(t.Name(), NewFieldIndexer(NewJSONPath("path.part")))

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - value added and removed using recursion", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewJSONPath("path.part")),
			NewFieldIndexer(NewJSONPath("path.more.#.parts")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("error - illegal document format", func(t *testing.T) {
		i := c.NewIndex(t.Name(), NewFieldIndexer(NewJSONPath("path.parts")))

		err := db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Delete(b, ref, []byte("}"))
		})

		assert.Error(t, err)
	})

	t.Run("ok - no match", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewJSONPath("path.part")),
			NewFieldIndexer(NewJSONPath("path.more.#.parts")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Delete(b, ref, []byte("{}"))
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - not indexed", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewJSONPath("path.part")),
			NewFieldIndexer(NewJSONPath("path.more.#.parts")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			return i.Delete(b, ref, doc)
		})

		assertIndexSize(t, db, i, 0)
	})

	t.Run("ok - multiple entries", func(t *testing.T) {
		i := c.NewIndex(t.Name(),
			NewFieldIndexer(NewJSONPath("path.part")),
			NewFieldIndexer(NewJSONPath("path.more.#.parts")),
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
	key := NewJSONPath("path.part")
	key2 := NewJSONPath("path.more.#.parts")
	i := c.NewIndex(t.Name(),
		NewFieldIndexer(key),
		NewFieldIndexer(key2),
	)

	t.Run("ok - exact match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(key, valueAsScalar)).
				And(Eq(key2, valueAsScalar)))

		assert.Equal(t, 2.0, f)
	})

	t.Run("ok - exact match reverse ordering", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(key2, valueAsScalar)).
				And(Eq(key, valueAsScalar)))

		assert.Equal(t, 2.0, f)
	})

	t.Run("ok - partial match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(key, valueAsScalar)))

		assert.Equal(t, 1.0, f)
	})

	t.Run("ok - no match", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(NewJSONPath("key3"), valueAsScalar)))

		assert.Equal(t, 0.0, f)
	})

	t.Run("ok - no match on second index only", func(t *testing.T) {
		f := i.IsMatch(
			New(Eq(key2, valueAsScalar)))

		assert.Equal(t, 0.0, f)
	})
}

func TestIndex_Find(t *testing.T) {
	doc := []byte(jsonExample)
	ref := defaultReferenceCreator(doc)
	doc2 := []byte(jsonExample2)
	ref2 := defaultReferenceCreator(doc2)
	key := NewJSONPath("path.part")
	key2 := NewJSONPath("path.parts")
	key3 := NewJSONPath("path.more.#.parts")
	db, c := testCollection(t)

	i := c.NewIndex(t.Name(),
		NewFieldIndexer(key, TokenizerOption(WhiteSpaceTokenizer), TransformerOption(ToLower)),
		NewFieldIndexer(key2),
		NewFieldIndexer(key3),
	)

	_ = db.Update(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		_ = i.Add(b, ref, doc)
		return i.Add(b, ref2, doc2)
	})

	t.Run("ok - not found", func(t *testing.T) {
		q := New(Eq(key, MustParseScalar("not_found")))
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
		q := New(Eq(key, valueAsScalar)).And(
			Eq(key2, MustParseScalar("value2"))).And(
			Eq(key3, MustParseScalar(1.0)))
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

	t.Run("ok - not nil", func(t *testing.T) {
		q := New(NotNil(key))
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

	t.Run("ok - not nil in compound index", func(t *testing.T) {
		q := New(Eq(key, valueAsScalar)).And(NotNil(key2))
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

	t.Run("ok - match through transformer", func(t *testing.T) {
		q := New(Eq(key, MustParseScalar("VALUE"))).And(
			Eq(key2, MustParseScalar("value2"))).And(
			Eq(key3, MustParseScalar(1.0)))
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
		q := New(Eq(key, valueAsScalar))

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
			NewFieldIndexer(key, TokenizerOption(WhiteSpaceTokenizer), TransformerOption(ToLower)),
			NewFieldIndexer(NewJSONPath("path.unknown")),
			NewFieldIndexer(NewJSONPath("path.unknown2")),
		)

		_ = db.Update(func(tx *bbolt.Tx) error {
			b := testBucket(t, tx)
			_ = i.Add(b, ref, doc)
			return i.Add(b, ref2, doc2)
		})

		q := New(Eq(key, valueAsScalar))

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
		q := New(Eq(key3, valueAsScalar))

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
	key := NewJSONPath("path.part")

	i := c.NewIndex(t.Name(),
		NewFieldIndexer(key, TokenizerOption(WhiteSpaceTokenizer), TransformerOption(ToLower)),
	).(*index)

	_ = db.Update(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		return i.Add(b, ref, doc)
	})

	q := New(Eq(key, valueAsScalar))
	matchers := i.matchers(q.parts)
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
			_, err := findR(cursor, []byte{}, matchers, foundFunc, []byte{}, 0)
			return err
		})

		assert.NoError(t, err)
		assert.True(t, found)
	})

	t.Run("skip match when cursor already further", func(t *testing.T) {
		found = false

		// by passing the value to be found as latest cursor value, it should skip over the results
		err := db.View(func(tx *bbolt.Tx) error {
			cursor := testBucket(t, tx).Bucket(i.BucketName()).Cursor()
			_, err := findR(cursor, []byte{}, matchers, foundFunc, []byte("valuf"), 0)
			return err
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

func TestIndex_matchingParts(t *testing.T) {
	_, c := testCollection(t)
	key := NewJSONPath("path.part")
	key2 := NewJSONPath("path.more.#.parts")
	i := c.NewIndex(t.Name(),
		NewFieldIndexer(key),
		NewFieldIndexer(key2),
	).(*index)

	t.Run("returns correct order when given in reverse", func(t *testing.T) {
		sorted := i.matchingParts(
			New(Eq(key2, valueAsScalar)).
				And(Eq(key, valueAsScalar)))

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, key, sorted[0].QueryPath())
		assert.Equal(t, key2, sorted[1].QueryPath())
	})

	t.Run("returns correct order when given in correct order", func(t *testing.T) {
		sorted := i.matchingParts(
			New(Eq(key, valueAsScalar)).
				And(Eq(key2, valueAsScalar)))

		if !assert.Len(t, sorted, 2) {
			return
		}
		assert.Equal(t, key, sorted[0].QueryPath())
		assert.Equal(t, key2, sorted[1].QueryPath())
	})

	t.Run("does not include any keys when primary key is missing", func(t *testing.T) {
		sorted := i.matchingParts(
			New(Eq(key2, valueAsScalar)))

		assert.Len(t, sorted, 0)
	})

	t.Run("returns first key when duplicate keys are given for index", func(t *testing.T) {
		sorted := i.matchingParts(
			New(Eq(key, valueAsScalar)).
				And(Eq(key, valueAsScalar2)))

		if !assert.Len(t, sorted, 1) {
			return
		}
		assert.True(t, sorted[0].Condition(Key("value"), nil))
	})
}

func TestIndex_QueryPartsOutsideIndex(t *testing.T) {
	key := NewJSONPath("path.part")
	key2 := NewJSONPath("path.more.#.parts")
	key3 := NewJSONPath("key3")

	_, c := testCollection(t)
	i := c.NewIndex(t.Name(),
		NewFieldIndexer(key),
		NewFieldIndexer(key2),
	).(*index)

	t.Run("returns empty list when all parts in index", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq(key2, valueAsScalar)).
				And(Eq(key, valueAsScalar)))

		assert.Len(t, additional, 0)
	})

	t.Run("returns all parts when none match index", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq(key2, valueAsScalar)))

		assert.Len(t, additional, 1)
	})

	t.Run("returns correct params on partial index match", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq(key3, valueAsScalar)).
				And(Eq(key, valueAsScalar)))

		if !assert.Len(t, additional, 1) {
			return
		}
		assert.Equal(t, key3, additional[0].QueryPath())
	})

	t.Run("returns param if duplicate and is index hit", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq(key, valueAsScalar)).
				And(Eq(key, valueAsScalar)))

		if !assert.Len(t, additional, 1) {
			return
		}
		assert.Equal(t, key, additional[0].QueryPath())
	})

	t.Run("returns all duplicates", func(t *testing.T) {
		additional := i.QueryPartsOutsideIndex(
			New(Eq(key, valueAsScalar)).
				And(Eq(key, valueAsScalar)).
				And(Eq(key3, valueAsScalar)).
				And(Eq(key3, valueAsScalar)))

		if !assert.Len(t, additional, 3) {
			return
		}
		assert.Equal(t, key, additional[0].QueryPath())
		assert.Equal(t, key3, additional[1].QueryPath())
		assert.Equal(t, key3, additional[2].QueryPath())
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
		ip := NewFieldIndexer(NewJSONPath("path.part"))
		keys, err := i.Keys(ip, document)

		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 1) {
			return
		}

		assert.Equal(t, "value", keys[0].value())
	})

	t.Run("ok - sub sub object", func(t *testing.T) {
		_, _, i := testIndex(t)
		ip := NewFieldIndexer(NewJSONPath("path.more.#.parts"))
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
		ip := NewFieldIndexer(NewJSONPath("path.parts"))
		keys, err := i.Keys(ip, document)

		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 2) {
			return
		}

		assert.Equal(t, "value1", keys[0].value())
		assert.Equal(t, "value2", keys[1].value())
	})

	t.Run("ok - no match", func(t *testing.T) {
		_, _, i := testIndex(t)
		ip := NewFieldIndexer(NewJSONPath("path.party"))
		keys, err := i.Keys(ip, document)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, keys, 0)
	})

	t.Run("error - incorrect document", func(t *testing.T) {
		_, _, i := testIndex(t)
		ip := NewFieldIndexer(NewJSONPath("path.part"))

		_, err := i.Keys(ip, []byte("}"))

		assert.Error(t, err)
	})
}
