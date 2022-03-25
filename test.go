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
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var jsonExample = `
{
	"path": {
		"part": "value",
		"parts": ["value1", "value3"],
		"more": [
			{
				"parts": 0.0
			}
		]
	},
	"non_indexed": "value"
}
`

var jsonExample2 = `
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

var jsonLDExample = `
{
  "@context": {
    "id": "@id",
    "type": "@type",
    "schema": "http://example.com/",
    "Person": {
      "@id": "schema:Person",
      "@context": {
        "id": "@id",
        "type": "@type",
        
        "name": {"@id": "schema:name"},
        "telephone": {"@id": "schema:telephone"},
        "url": {"@id": "schema:url"},
        "children": {"@id": "schema:children", "@type": "@id"}
      }
    }
  },
  "@type": "Person",
  "name": "Jane Doe",
  "url": "http://www.janedoe.com",
  "children": [{
    "@type": "Person",
    "name": "John Doe",
	"url": "http://www.johndoe.org"
  }]
}
`

var invalidPathCharRegex = regexp.MustCompile("([^a-zA-Z0-9])")

// testDirectory returns a temporary directory for this test only. Calling TestDirectory multiple times for the same
// instance of t returns a new directory every time.
func testDirectory(t *testing.T) string {
	if dir, err := ioutil.TempDir("", normalizeTestName(t)); err != nil {
		t.Fatal(err)
		return ""
	} else {
		t.Cleanup(func() {
			if err := os.RemoveAll(dir); err != nil {
				_, _ = os.Stderr.WriteString(fmt.Sprintf("Unable to remove temporary directory for test (%s): %v\n", dir, err))
			}
		})
		return dir
	}
}

type testFunc func(bucket *bbolt.Bucket) error

func withinBucket(t *testing.T, db *bbolt.DB, fn testFunc) error {
	return db.Update(func(tx *bbolt.Tx) error {
		bucket := testBucket(t, tx)
		return fn(bucket)
	})
}

func testDB(t *testing.T) *bbolt.DB {
	db, err := bbolt.Open(filepath.Join(testDirectory(t), "test.db"), boltDBFileMode, &bbolt.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func testBucket(t *testing.T, tx *bbolt.Tx) *bbolt.Bucket {
	if tx.Writable() {
		bucket, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			t.Fatal(err)
		}
		return bucket
	}
	return tx.Bucket([]byte("test"))
}

func normalizeTestName(t *testing.T) string {
	return invalidPathCharRegex.ReplaceAllString(t.Name(), "_")
}

// assertIndexed checks if a key/value has been indexed
func assertIndexed(t *testing.T, db *bbolt.DB, i Index, key []byte, ref Reference) bool {
	err := db.View(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		b = b.Bucket(i.BucketName())
		sub := b.Bucket(key)

		cursor := sub.Cursor()
		for k, _ := cursor.Seek([]byte{}); k != nil; k, _ = cursor.Next() {
			if bytes.Compare(ref, k) == 0 {
				return nil
			}
		}

		return errors.New("ref not found")
	})

	return assert.NoError(t, err)
}

// assertIndexSize checks if an index has a certain size
func assertIndexSize(t *testing.T, db *bbolt.DB, i Index, size int) bool {
	err := db.Update(func(tx *bbolt.Tx) error {
		b := testBucket(t, tx)
		b = b.Bucket(i.BucketName())

		if b == nil {
			if size == 0 {
				return nil
			}
			return errors.New("empty bucket")
		}
		count := 0
		// loop over sub-buckets
		cursor := b.Cursor()
		for k, _ := cursor.Seek([]byte{}); k != nil; k, _ = cursor.Next() {
			subBucket := b.Bucket(k)
			subCursor := subBucket.Cursor()
			for k2, _ := subCursor.Seek([]byte{}); k2 != nil; k2, _ = subCursor.Next() {
				count++
			}
		}

		assert.Equal(t, size, count)
		return nil
	})

	return assert.NoError(t, err)
}

// assertSize checks a bucket size
func assertSize(t *testing.T, db *bbolt.DB, bucketName string, size int) bool {
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("test"))
		if b == nil {
			if size == 0 {
				return nil
			}
			panic("missing bucket")
		}
		b = b.Bucket([]byte(bucketName))
		if b == nil {
			if size == 0 {
				return nil
			}
			panic("missing bucket")
		}
		assert.Equal(t, size, b.Stats().KeyN)
		return nil
	})

	return assert.NoError(t, err)
}

func toBytes(data interface{}) ([]byte, error) {
	switch castData := data.(type) {
	case []uint8:
		return castData, nil
	case uint32:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], castData)
		return buf[:], nil
	case string:
		return []byte(castData), nil
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(castData))
		return buf[:], nil
	}

	return nil, errors.New("couldn't convert data to []byte")
}
