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
	"bytes"
	"github.com/nuts-foundation/go-stoabs"
)

// KeyOf creates a key from an interface
func KeyOf(value interface{}) stoabs.BytesKey {
	switch val := value.(type) {
	case string:
		return []byte(val)
	case []byte:
		return val
	case stoabs.BytesKey:
		return val
	}
	return nil
}

// ComposeKey creates a new key from two keys
func ComposeKey(current stoabs.BytesKey, additional stoabs.BytesKey) stoabs.BytesKey {
	if len(current) == 0 {
		return additional.Bytes()
	}

	c := Split(current)
	b := make([][]byte, len(c))
	for i, k := range c {
		b[i] = k
	}

	b = append(b, additional)
	return bytes.Join(b, []byte{KeyDelimiter})
}

// Split splits a compound key into parts
func Split(key stoabs.BytesKey) []stoabs.BytesKey {
	s := bytes.Split(key, []byte{KeyDelimiter})
	var nk = make([]stoabs.BytesKey, len(s))

	for i, si := range s {
		nk[i] = si
	}

	return nk
}
