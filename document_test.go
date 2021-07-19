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

func TestDocumentFromBytes(t *testing.T) {
	bytes := []byte("test")

	doc := DocumentFromBytes(bytes)

	assert.Equal(t, bytes, doc.raw)
}

func TestDocumentFromString(t *testing.T) {
	bytes := []byte("test")

	doc := DocumentFromString("test")

	assert.Equal(t, bytes, doc.raw)
}

func TestDocument_ValuesAtPath(t *testing.T) {
	json := `
{
	"id": 1,
	"name": "test",
	"colors": ["blue", "orange"],
	"items" : [
		{
			"type": "car",
			"count": 2
		},
		{
			"type": "bike",
			"count": 5
		}
	],
	"animals": [
		{
			"nesting": {
				"type": "bird"
			}
		}
	]
}
`

	document := DocumentFromString(json)

	t.Run("ok - find a single float value", func(t *testing.T) {
		values, err := document.ValuesAtPath("id")

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 1)
		assert.Equal(t, 1.0, values[0])
	})

	t.Run("ok - find a single string value", func(t *testing.T) {
		values, err := document.ValuesAtPath("name")

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 1)
		assert.Equal(t, "test", values[0])
	})

	t.Run("ok - find a list of values", func(t *testing.T) {
		values, err := document.ValuesAtPath("colors")

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 2)
		assert.Equal(t, "blue", values[0])
		assert.Equal(t, "orange", values[1])
	})

	t.Run("ok - find a list of values from a sublist", func(t *testing.T) {
		values, err := document.ValuesAtPath("items.#.type")

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 2)
		assert.Equal(t, "car", values[0])
		assert.Equal(t, "bike", values[1])
	})

	t.Run("ok - values at an unknown path", func(t *testing.T) {
		values, err := document.ValuesAtPath("unknown")

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 0)
	})

	t.Run("error - invalid json", func(t *testing.T) {
		_, err := DocumentFromString("{").ValuesAtPath("id")

		assert.Equal(t, ErrInvalidJSON, err)
	})

	t.Run("error - indexing an object", func(t *testing.T) {
		_, err := document.ValuesAtPath("animals.#.nesting")

		assert.EqualError(t, err, "type at path not supported for indexing: {\n\t\t\t\t\"type\": \"bird\"\n\t\t\t}")
	})
}

func TestDocument_KeysAtPath(t *testing.T) {
	json := `
{
	"id": 1,
	"name": "test",
	"colors": ["blue", "orange"],
	"items" : [
		{
			"type": "car",
			"count": 2
		},
		{
			"type": "bike",
			"count": 5
		}
	],
	"animals": [
		{
			"nesting": {
				"type": "bird",
				"nice": false
			}
		}
	]
}
`

	document := DocumentFromString(json)

	t.Run("ok - find a list of keys from a sublist", func(t *testing.T) {
		values, err := document.KeysAtPath("items.#.type")

		if !assert.NoError(t, err) {
			return
		}

		assert.Len(t, values, 2)
		assert.Equal(t, Key("car"), values[0])
		assert.Equal(t, Key("bike"), values[1])
	})

	t.Run("error - invalid json", func(t *testing.T) {
		_, err := DocumentFromString("{").KeysAtPath("id")

		assert.Equal(t, ErrInvalidJSON, err)
	})
}
