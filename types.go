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
	"encoding/hex"
	"errors"
	"math"
)

const boltDBFileMode = 0600
const KeyDelimiter = 0x10

// Document represents a JSON document in []byte format
type Document []byte

// ErrInvalidJSON is returned when invalid JSON is parsed
var ErrInvalidJSON = errors.New("invalid json")

// Reference equals a document hash. In an index, the values are references to docs.
type Reference []byte

// EncodeToString encodes the reference as hex encoded string
func (r Reference) EncodeToString() string {
	return hex.EncodeToString(r)
}

// ByteSize returns the size of the reference, eg: 32 bytes for a sha256
func (r Reference) ByteSize() int {
	return len(r)
}

// Scalar represents a JSON or JSON-LD scalar (string, number, true or false)
type Scalar struct {
	value interface{}
}

// ErrInvalidValue is returned when an invalid value is parsed
var ErrInvalidValue = errors.New("invalid value")

// ScalarParse returns a Scalar based on an interface value. It returns ErrInvalidValue for unsupported values.
func ScalarParse(value interface{}) (Scalar, error) {
	switch value.(type) {
	case bool:
		return Scalar{value: value}, nil
	case string:
		return Scalar{value: value}, nil
	case float64:
		return Scalar{value: value}, nil
	}
	// not possible
	return Scalar{}, ErrInvalidValue
}

// ScalarMustParse returns a Scalar based on an interface value. It panics when the value is not supported.
func ScalarMustParse(value interface{}) Scalar {
	s, err := ScalarParse(value)
	if err != nil {
		panic(err)
	}
	return s
}

// Bytes returns the byte representation of the Scalar
func (s Scalar) Bytes() []byte {
	switch castData := s.value.(type) {
	case bool:
		if castData {
			return []byte{1}
		}
		return []byte{0}
	case string:
		return []byte(castData)
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(castData))
		return buf[:]
	}

	return []byte{}
}
