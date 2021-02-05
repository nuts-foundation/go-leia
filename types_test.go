/*
 * go-leia
 * Copyright (C) 2021 Nuts community
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package leia

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDocument(t *testing.T) {
	d := NewReference([]byte("hello"))
	h := hex.EncodeToString(d)

	assert.Equal(t, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", h)
}

func TestReference_EncodeToString(t *testing.T) {
	ref := Reference("ref")
	h := ref.EncodeToString()

	assert.Equal(t, "726566", h)
}

func TestReference_ByteSize(t *testing.T) {
	ref := Reference("ref")

	assert.Equal(t, 3, ref.ByteSize())
}

func TestComposeKey(t *testing.T) {
	t.Run("ok - empty keys", func(t *testing.T) {
		k := ComposeKey(nil, nil)

		assert.Nil(t, k)
	})

	t.Run("ok - initial key", func(t *testing.T) {
		a := Key("additional")
		k := ComposeKey(nil, a)

		assert.Equal(t, a, k)
	})

	t.Run("ok - multiple key", func(t *testing.T) {
		k1 := Key("first")
		k2 := Key("second")
		exp := Key(fmt.Sprintf("first%csecond", KeyDelimiter))

		k := ComposeKey(k1, k2)

		assert.Equal(t, exp, k)
	})
}

func TestKey_Split(t *testing.T) {
	t.Run("ok - single key", func(t *testing.T) {
		s := Key("first").Split()

		assert.Len(t, s, 1)
	})

	t.Run("ok - multiple keys", func(t *testing.T) {
		k1 := Key("first")
		k2 := Key("second")
		c := Key(fmt.Sprintf("first%csecond", KeyDelimiter))

		s := c.Split()

		assert.Len(t, s, 2)
		assert.Equal(t, k1, s[0])
		assert.Equal(t, k2, s[1])
	})
}

func TestToBytes(t *testing.T) {
	t.Run("ok - float", func(t *testing.T) {
		s := 0.0

		b, err := toBytes(s)

		if !assert.NoError(t, err) {
			return
		}

		bits := binary.BigEndian.Uint64(b)
		fl := math.Float64frombits(bits)

		assert.Equal(t, s, fl)
	})

	t.Run("ok - string", func(t *testing.T) {
		s := "test"

		b, err := toBytes(s)

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, s, string(b))
	})

	t.Run("error - unknown type", func(t *testing.T) {
		s := 0

		_, err := toBytes(s)

		assert.Error(t, err)
	})
}
