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

	"github.com/piprate/json-gold/ld"
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

func TestStore_JSONCollection(t *testing.T) {
	f := filepath.Join(testDirectory(t), "test.db")
	s, _ := NewStore(f, WithoutSync())

	c := s.JSONCollection("test")

	if !assert.NotNil(t, c) {
		return
	}

	assert.NotNil(t, c.(*collection).db)
	assert.NotNil(t, c.(*collection).refMake)
	assert.NotNil(t, c.(*collection).name)
	assert.NotNil(t, c.(*collection).valueCollector)
}

func TestStore_JSONLDCollection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		f := filepath.Join(testDirectory(t), "test.db")
		s, _ := NewStore(f, WithoutSync())

		c := s.JSONLDCollection("test")

		if !assert.NotNil(t, c) {
			return
		}

		assert.NotNil(t, c.(*collection).db)
		assert.NotNil(t, c.(*collection).refMake)
		assert.NotNil(t, c.(*collection).name)
		assert.NotNil(t, c.(*collection).documentLoader)
		assert.NotNil(t, c.(*collection).valueCollector)
	})

	t.Run("custom documentLoader", func(t *testing.T) {
		f := filepath.Join(testDirectory(t), "test.db")
		s, _ := NewStore(f, WithoutSync(), WithDocumentLoader(testDocumentLoader{}))

		c := s.JSONLDCollection("test")

		if !assert.NotNil(t, c) {
			return
		}

		_, ok := c.(*collection).documentLoader.(testDocumentLoader)
		assert.True(t, ok)
	})
}

type testDocumentLoader struct{}

func (t testDocumentLoader) LoadDocument(u string) (*ld.RemoteDocument, error) {
	return nil, nil
}
