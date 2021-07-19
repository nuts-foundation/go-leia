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

func TestReference_EncodeToString(t *testing.T) {
	ref := Reference("ref")
	h := ref.EncodeToString()

	assert.Equal(t, "726566", h)
}

func TestReference_ByteSize(t *testing.T) {
	ref := Reference("ref")

	assert.Equal(t, 3, ref.ByteSize())
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
