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

import "bytes"

// Key is used as DB key type
type Key []byte

// KeyOf creates a key from an interface
func KeyOf(value interface{}) Key {
	switch value.(type) {
	case string:
		return []byte(value.(string))
	case []byte:
		return value.([]byte)
	case Key:
		return value.(Key)
	}
	return nil
}

// String returns the string representation, only useful if a Key represents readable bytes
func (k Key) String() string {
	return string(k)
}

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

