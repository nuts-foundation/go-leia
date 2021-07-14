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
	// The optional transform fn is applied to this query part before evaluation is done.
	Condition(key Key, transform Transform) (bool, error)
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

// Prefix creates a query part for a partial match
// The beginning of a value is matched against the query.
func Prefix(name string, value interface{}) QueryPart {
	return prefixPart{
		name:  name,
		value: value,
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

func (e eqPart) Condition(key Key, transform Transform) (bool, error) {
	if transform != nil {
		transformed := transform(e.value)
		transformedBytes, err := toBytes(transformed)
		if err != nil {
			return false, err
		}
		return bytes.Compare(key, transformedBytes) == 0, nil
	}

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

func (r rangePart) Condition(key Key, _ Transform) (bool, error) {
	b, err := toBytes(r.begin)
	if err != nil {
		return false, err
	}

	// the key becomes before the start
	if bytes.Compare(key, b) < 0 {
		return false, nil
	}

	b, err = toBytes(r.end)
	if err != nil {
		return false, err
	}
	return bytes.Compare(key, b) <= 0, nil
}

type prefixPart struct {
	name  string
	value interface{}
}

func (p prefixPart) Name() string {
	return p.name
}

func (p prefixPart) Seek() (Key, error) {
	return toBytes(p.value)
}

func (p prefixPart) Condition(key Key, transform Transform) (bool, error) {
	transformed := p.value
	if transform != nil {
		transformed = transform(p.value)
	}

	prefix, err := toBytes(transformed)
	if err != nil {
		return false, err
	}

	return bytes.HasPrefix(key, prefix), nil
}

