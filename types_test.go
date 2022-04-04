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

func TestParseScalar(t *testing.T) {
	t.Run("ok - string", func(t *testing.T) {
		s, err := ParseScalar("string")

		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, "string", s.value())
	})

	t.Run("ok - number", func(t *testing.T) {
		s, err := ParseScalar(1.0)

		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, 1.0, s.value())
	})

	t.Run("ok - true", func(t *testing.T) {
		s, err := ParseScalar(true)

		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, true, s.value())
	})

	t.Run("ok - false", func(t *testing.T) {
		s, err := ParseScalar(false)

		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, false, s.value())
	})

	t.Run("err - unsupported", func(t *testing.T) {
		_, err := ParseScalar(struct{}{})

		assert.Equal(t, ErrInvalidValue, err)
	})
}

func TestScalar_Bytes(t *testing.T) {
	t.Run("ok - string", func(t *testing.T) {
		s := StringScalar("string")

		assert.Equal(t, []byte("string"), s.Bytes())
	})

	t.Run("ok - number", func(t *testing.T) {
		s := Float64Scalar(1.0)

		assert.Equal(t, []byte{0x3f, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, s.Bytes())
	})

	t.Run("ok - negative number", func(t *testing.T) {
		s := Float64Scalar(-1.0)

		assert.Equal(t, []byte{0xbf, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, s.Bytes())
	})

	t.Run("ok - 0", func(t *testing.T) {
		s := Float64Scalar(0.0)

		assert.Equal(t, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, s.Bytes())
	})

	t.Run("ok - true", func(t *testing.T) {
		s := BoolScalar(true)

		assert.Equal(t, []byte{0x01}, s.Bytes())
	})

	t.Run("ok - false", func(t *testing.T) {
		s := BoolScalar(false)

		assert.Equal(t, []byte{0x0}, s.Bytes())
	})
}
