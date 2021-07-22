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
)

func TestNewIndexPart(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		ip := NewFieldIndexer("path", AliasOption("name"))

		jip, ok := ip.(fieldIndexer)

		if !assert.True(t, ok) {
			return
		}
		assert.Equal(t, "name", jip.Name())
		assert.Equal(t, "path", jip.path)
	})
}

func TestJsonIndexPart_Name(t *testing.T) {
	t.Run("ok - by alias", func(t *testing.T) {
		ip := NewFieldIndexer("path", AliasOption("name"))

		assert.Equal(t, "name", ip.Name())
	})

	t.Run("ok - by path", func(t *testing.T) {
		ip := NewFieldIndexer("path")

		assert.Equal(t, "path", ip.Name())
	})
}

func TestJsonIndexPart_Keys(t *testing.T) {
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
	document := Document{raw: []byte(json)}

	t.Run("ok - sub object", func(t *testing.T) {
		ip := NewFieldIndexer("path.part")
		keys, err := ip.Keys(document)

		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 1) {
			return
		}

		assert.Equal(t, "value", string(keys[0]))
	})

	t.Run("ok - sub sub object", func(t *testing.T) {
		ip := NewFieldIndexer("path.more.#.parts")
		keys, err := ip.Keys(document)

		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 1) {
			return
		}

		bits := binary.BigEndian.Uint64(keys[0])
		fl := math.Float64frombits(bits)

		assert.Equal(t, 0.0, fl)
	})

	t.Run("ok - list", func(t *testing.T) {
		ip := NewFieldIndexer("path.parts")
		keys, err := ip.Keys(document)

		if !assert.NoError(t, err) {
			return
		}

		if !assert.Len(t, keys, 2) {
			return
		}

		assert.Equal(t, "value1", string(keys[0]))
		assert.Equal(t, "value2", string(keys[1]))
	})

	t.Run("ok - no match", func(t *testing.T) {
		ip := NewFieldIndexer("path.party")
		keys, err := ip.Keys(document)

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, keys, 0)
	})

	t.Run("error - incorrect document", func(t *testing.T) {
		ip := NewFieldIndexer("path.part")
		_, err := ip.Keys(Document{raw: []byte("}")})

		assert.Error(t, err)
	})
}
