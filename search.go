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
)

type Query interface {
	// And adds a condition to query on
	And(part QueryPart) Query

	// Parts returns the different parts of the query
	Parts() []QueryPart
}

type QueryPart interface {

	// Name returns the name that matches the index part
	Name() string

	// Seek returns the key for cursor.Seek
	Seek() (Key, error)

	// Condition returns true if given key falls within this condition.
	Condition(key Key) (bool, error)
}

// New creates a new query with an initial query part. Both begin and end are inclusive for the conditional check.
func New(part QueryPart) Query {
	return query{
		parts: []QueryPart{part},
	}
}

// Eq creates a query part for an exact match
func Eq(name string, value interface{}) QueryPart {
	return eqPart{
		name:  name,
		value: value,
	}
}

// Range creates a query part for a range query
func Range(name string, begin interface{}, end interface{}) QueryPart {
	return rangePart{
		name:  name,
		begin: begin,
		end:   end,
	}
}

type query struct {
	parts []QueryPart
}

func (q query) And(part QueryPart) Query {
	q.parts = append(q.parts, part)
	return q
}

func (q query) Parts() []QueryPart {
	return q.parts
}

type eqPart struct {
	name  string
	value interface{}
}

func (e eqPart) Name() string {
	return e.name
}

func (e eqPart) Seek() (Key, error) {
	return toBytes(e.value)
}

func (e eqPart) Condition(key Key) (bool, error) {
	b, err := toBytes(e.value)
	if err != nil {
		return false, err
	}
	return bytes.Compare(key, b) == 0, nil
}

type rangePart struct {
	name  string
	begin interface{}
	end   interface{}
}

func (r rangePart) Name() string {
	return r.name
}

func (r rangePart) Seek() (Key, error) {
	return toBytes(r.begin)
}

func (r rangePart) Condition(key Key) (bool, error) {
	b, err := toBytes(r.end)
	if err != nil {
		return false, err
	}
	return bytes.Compare(key, b) <= 0, nil
}
