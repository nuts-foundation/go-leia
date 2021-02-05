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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
)

const boltDBFileMode = 0600
const documents = "documents"
const KeyDelimiter = 0x10

// Document represents a JSON document in []byte format
type Document []byte

// String returns the document in string format
func (d Document) String() string {
	return string(d)
}

// todo: mvoe to collection
// Reference returns the reference of the document
func (d Document) Reference() Reference {
	return NewReference(d)
}

// Key is used as DB key type
type Key []byte

// todo: determine delimiter byte: data link escape character: 0x10
// ComposeKey creates a new key from two keys
func ComposeKey(current Key, additional Key) Key {
	if len(current) == 0 {
		return additional
	}

	c := current.Split()
	b := make([][]byte, len(c))
	for i, k := range c {
		b[i] = k
	}

	b = append(b, additional)
	return bytes.Join(b, []byte{KeyDelimiter})
}

// Split splits a compound key into parts
func (k Key) Split() []Key {
	s := bytes.Split(k, []byte{KeyDelimiter})
	var nk = make([]Key, len(s))

	for i, si := range s {
		nk[i] = si
	}

	return nk
}

// todo: move to collection
// NewReference calculates the sha256 of a piece of data and returns it as reference type
func NewReference(data []byte) Reference {
	s := sha256.Sum256(data)
	var b = make([]byte, 32)
	copy(b, s[:])

	return b
}

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

func toBytes(data interface{}) ([]byte, error) {
	switch data.(type) {
	case string:
		return []byte(data.(string)), nil
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(data.(float64)))
		return buf[:], nil
	}

	return nil, errors.New("couldn't convert data to []byte")
}