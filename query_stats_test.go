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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryStatsCallbacks_Integration(t *testing.T) {
	dir, err := ioutil.TempDir("", "query-stats-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	var indexStatsCalls []IndexStats

	callbacks := QueryStatsCallbacks{
		OnIndexProblem: func(stats IndexStats) {
			indexStatsCalls = append(indexStatsCalls, stats)
		},
		SuboptimalIndexThreshold: 3,
	}

	s, err := NewStore(filepath.Join(dir, "test.db"), WithQueryStatsCallbacks(callbacks))
	require.NoError(t, err)
	defer s.Close()

	c := s.Collection(JSONCollection, "test")

	nameIndex := c.NewIndex("name_index", NewFieldIndexer(NewJSONPath("name")))
	require.NoError(t, c.AddIndex(nameIndex))

	docs := []Document{
		[]byte(`{"name": "Alice", "age": 30, "city": "NYC"}`),
		[]byte(`{"name": "Alice", "age": 25, "city": "LA"}`),
		[]byte(`{"name": "Alice", "age": 35, "city": "SF"}`),
		[]byte(`{"name": "Alice", "age": 40, "city": "Boston"}`),
		[]byte(`{"name": "Alice", "age": 45, "city": "Seattle"}`),
		[]byte(`{"name": "Bob", "age": 30, "city": "NYC"}`),
	}
	require.NoError(t, c.Add(docs))

	t.Run("full table scan callback is called", func(t *testing.T) {
		indexStatsCalls = nil

		query := New(Eq(NewJSONPath("city"), MustParseScalar("NYC")))
		_, err := c.Find(context.Background(), query)
		require.NoError(t, err)

		assert.Len(t, indexStatsCalls, 1)
		stats := indexStatsCalls[0]
		assert.Equal(t, "test", stats.Collection)
		assert.Equal(t, 6, stats.DocumentsScanned)
		assert.Equal(t, 2, stats.DocumentsMatched)
		assert.Greater(t, stats.ResultSetBytes, 0)
		assert.Len(t, stats.SuggestedFields, 1)
		// Full table scan - check zero values
		assert.Equal(t, "", stats.IndexUsed)
		assert.Equal(t, 0, stats.QueryPartsInIndex)
		assert.Equal(t, 1, stats.QueryPartsOutsideIndex)
		assert.Equal(t, 0.0, stats.FilterEfficiency)
	})

	t.Run("suboptimal index callback is called above threshold", func(t *testing.T) {
		indexStatsCalls = nil

		query := New(Eq(NewJSONPath("name"), MustParseScalar("Alice"))).
			And(Eq(NewJSONPath("city"), MustParseScalar("NYC")))
		_, err := c.Find(context.Background(), query)
		require.NoError(t, err)

		assert.Len(t, indexStatsCalls, 1)
		stats := indexStatsCalls[0]
		assert.Equal(t, "test", stats.Collection)
		assert.Equal(t, "name_index", stats.IndexUsed)
		assert.Equal(t, 1, stats.QueryPartsInIndex)
		assert.Equal(t, 1, stats.QueryPartsOutsideIndex)
		assert.Equal(t, 5, stats.DocumentsScanned)
		assert.Equal(t, 1, stats.DocumentsMatched)
		assert.Greater(t, stats.ResultSetBytes, 0)
		assert.Equal(t, 0.2, stats.FilterEfficiency)
		assert.Len(t, stats.SuggestedFields, 2)
	})

	t.Run("no callback below threshold", func(t *testing.T) {
		indexStatsCalls = nil

		query := New(Eq(NewJSONPath("name"), MustParseScalar("Bob"))).
			And(Eq(NewJSONPath("city"), MustParseScalar("NYC")))
		_, err := c.Find(context.Background(), query)
		require.NoError(t, err)

		assert.Len(t, indexStatsCalls, 0, "No callback should be triggered below threshold")
	})

	t.Run("no callback for empty query (scan-all)", func(t *testing.T) {
		indexStatsCalls = nil

		// Empty query (no parts) - intentional scan-all, not a performance problem
		query := Query{parts: []QueryPart{}}
		_, err := c.Find(context.Background(), query)
		require.NoError(t, err)

		assert.Len(t, indexStatsCalls, 0, "No callback should be triggered for intentional scan-all query")
	})
}
