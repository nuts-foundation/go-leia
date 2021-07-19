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
	"fmt"
	"io/ioutil"
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
	db, err := bbolt.Open(filepath.Join(testDirectory(t), "test.db"), boltDBFileMode, bbolt.DefaultOptions)
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

		assert.Equal(t, size, b.Stats().KeyN)
		return nil
	})

	return assert.NoError(t, err)
}

// assertSize checks a bucket size
func assertSize(t *testing.T, db *bbolt.DB, bucketName string, size int) bool {
	err := db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			panic(err)
		}
		assert.Equal(t, size, b.Stats().KeyN)
		return nil
	})

	return assert.NoError(t, err)
}
