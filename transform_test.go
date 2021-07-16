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

// this file tests indexing and finding using a transformer
// to test the adding and searching capabilities, we use a string based document.
// the test based index returns each word as a key.

type testIndexPart struct {
	name string
	transformer Transform
	tokenizer Tokenizer
}

func (t testIndexPart) Name() string {
	return t.name
}

func (t testIndexPart) Keys(document Document) ([]Key, error) {
	words := t.tokenizer(string(document.raw))
	keys := make([]Key, len(words))
	for i, w := range words {
		transformed := t.Transform(w)
		keys[i] = []byte(transformed.(string))
	}
	return keys, nil
}

func (t testIndexPart) Tokenize(value interface{}) []interface{} {
	if t.tokenizer == nil {
		return []interface{}{value}
	}
	if s, ok := value.(string); ok {
		tokens := t.tokenizer(s)
		result := make([]interface{}, len(tokens))
		for i, t := range tokens {
			result[i] = t
		}
		return result
	}
	return []interface{}{value}
}

func (t testIndexPart) Transform(value interface{}) interface{} {
	if t.transformer == nil {
		return value
	}
	if s, ok := value.(string); ok {
		return t.transformer(s)
	}
	return value
}

func (t testIndexPart) Transformer() Transform {
	return t.transformer
}

func TestIndex_Add(t *testing.T) {
	i := NewIndex("test", testIndexPart{name: "part", tokenizer: WhiteSpaceTokenizer, transformer: ToLower})
	db := testDB(t)

	t.Run("ok - single word", func(t *testing.T) {
		ref := []byte("01")
		doc := []byte("WORD")
		key := []byte("word")

		err := withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Add(bucket, ref, Document{raw:doc})
		})

		assert.NoError(t, err)

		assertIndexed(t, db, i, key, ref)
	})

	t.Run("ok - sentence", func(t *testing.T) {
		ref := []byte("01")
		doc := []byte("WORD1 WORD2")
		key1 := []byte("word1")
		key2 := []byte("word2")

		err := withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Add(bucket, ref, Document{raw:doc})
		})

		assert.NoError(t, err)

		assertIndexed(t, db, i, key1, ref)
		assertIndexed(t, db, i, key2, ref)
	})
}

func TestIndex_Iterate(t *testing.T) {
	i := NewIndex("test", testIndexPart{name: "part", tokenizer: WhiteSpaceTokenizer, transformer: ToLower})

	t.Run("ok - single word", func(t *testing.T) {
		db := testDB(t)

		ref := []byte("01")
		doc := []byte("WORD")
		key := []byte("word")

		err := withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Add(bucket, ref, Document{raw:doc})
		})

		if !assert.NoError(t, err) {
			return
		}

		q := New(Eq("part", key))
		count := 0

		err = withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Iterate(bucket, q, func(key []byte, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("ok - sentence", func(t *testing.T) {
		db := testDB(t)

		ref := []byte("01")
		doc := []byte("WORD1 WORD2")
		key2 := []byte("word2")

		err := withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Add(bucket, ref, Document{raw:doc})
		})

		if !assert.NoError(t, err) {
			return
		}

		q := New(Eq("part", key2))
		count := 0

		err = withinBucket(t, db, func(bucket *bbolt.Bucket) error {
			return i.Iterate(bucket, q, func(key []byte, value []byte) error {
				count++
				return nil
			})
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}

func TestWhiteSpaceTokenizer(t *testing.T) {
	t.Run("ok - consecutive whitespace", func(t *testing.T) {
		tokens := WhiteSpaceTokenizer("WORD1 WORD2")

		assert.Len(t, tokens, 2)
	})
}
