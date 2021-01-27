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
	"math"
)

// SearchOption is the interface for defining which index to use for searching
type SearchOption interface {
	// Index corresponds to the name of an index
	Index() string
	// Value to be searched on
	Value() []byte
}

// StringSearchOption holds search options for a query on a string
type StringSearchOption struct {
	index string
	value string
}

func (s StringSearchOption) Index() string {
	return s.index
}

func (s StringSearchOption) Value() []byte {
	return []byte(s.value)
}

// FloatSearchOption holds search options for a query on a number
type FloatSearchOption struct {
	index string
	value float64
}

func (f FloatSearchOption) Index() string {
	return f.index
}

func (f FloatSearchOption) Value() []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(f.value))
	return buf[:]
}
