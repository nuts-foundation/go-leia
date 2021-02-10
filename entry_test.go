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
)

func TestEntryFrom(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		entry := EntryFrom(Reference("hello"))

		assert.Equal(t, 5, entry.RefSize)
		assert.Equal(t, 1, entry.Size())
	})
}

func TestEntry_Add(t *testing.T) {
	entry := EntryFrom(Reference("hello"))

	t.Run("error - wrong size", func(t *testing.T) {
		err := entry.Add(Reference("hello again"))

		assert.Error(t, err)
	})

	t.Run("ok", func(t *testing.T) {
		err := entry.Add(Reference("hell2"))

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, 2, entry.Size())
	})
}

func TestEntry_Delete(t *testing.T) {
	entry := EntryFrom(Reference("hello"))

	t.Run("ok", func(t *testing.T) {
		entry.Delete(Reference("hello"))

		assert.Equal(t, 0, entry.Size())
	})
}

func TestEntry_Slice(t *testing.T) {
	entry := EntryFrom(Reference("hello"))

	t.Run("ok", func(t *testing.T) {
		slice := entry.Slice()
		assert.Len(t, slice, 1)
	})

	t.Run("ok - 2 entries", func(t *testing.T) {
		err := entry.Add(Reference("hell2"))

		if !assert.NoError(t, err) {
			return
		}

		slice := entry.Slice()
		assert.Len(t, slice, 2)
	})
}

func TestEntry_Marshal(t *testing.T) {
	entry := EntryFrom(Reference("hello"))

	t.Run("ok", func(t *testing.T) {
		marshalled, err := entry.Marshal()

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, marshalled, 7)
		assert.Equal(t, "5", string(marshalled[0]))
	})
}

func TestEntry_Unmarshal(t *testing.T) {
	marshalled := []byte("5#hello")

	t.Run("ok", func(t *testing.T) {
		var entry Entry

		err := entry.Unmarshal(marshalled)

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, 5, entry.RefSize)
		assert.Equal(t, 1, entry.Size())
	})
}
