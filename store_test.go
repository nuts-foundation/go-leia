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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStore(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		f := filepath.Join(testDirectory(t), "test.db")
		s, err := NewStore(f, WithoutSync())

		if !assert.NoError(t, err) {
			return
		}

		assert.NotNil(t, s)
	})

	t.Run("error", func(t *testing.T) {
		_, err := NewStore("store_test.go", WithoutSync())

		assert.Error(t, err)
	})
}

func TestStore_Collection(t *testing.T) {
	f := filepath.Join(testDirectory(t), "test.db")
	s, _ := NewStore(f, WithoutSync())

	c := s.JsonCollection("test")

	if !assert.NotNil(t, c) {
		return
	}

	t.Run("db is set", func(t *testing.T) {
		assert.NotNil(t, c.(*jsonCollection).db)
	})

	t.Run("refMake is set", func(t *testing.T) {
		assert.NotNil(t, c.(*jsonCollection).refMake)
	})

	t.Run("path is set", func(t *testing.T) {
		assert.NotNil(t, c.(*jsonCollection).name)
	})

	t.Run("collections are stored in instance", func(t *testing.T) {
		_, c := testCollection(t)

		assert.Len(t, c.indexList, 0)
		c.AddIndex(c.NewIndex("test", NewFieldIndexer(NewJSONPath("path"))))

		assert.Len(t, c.indexList, 1)
	})
}
