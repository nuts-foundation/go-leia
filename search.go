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
	"errors"
)

// ErrNoQuery is returned when an empty query is given
var ErrNoQuery = errors.New("no query given")

type Query interface {
	// And adds a condition to query on
	And(part QueryPart) Query

	// Parts returns the different parts of the query
	Parts() []QueryPart
}

type QueryPart interface {

	// Name returns the name that matches fieldIndexer.Name() so actually the alias or JSON path
	Name() string

	// Seek returns the key for cursor.Seek
	Seek() Scalar

	// Condition returns true if given key falls within this condition.
	// The optional transform fn is applied to this query part before evaluation is done.
	Condition(key Key, transform Transform) bool
}

// New creates a new query with an initial query part. Both begin and end are inclusive for the conditional check.
func New(part QueryPart) Query {
	return query{
		parts: []QueryPart{part},
	}
}

// Eq creates a query part for an exact match
func Eq(name string, value Scalar) QueryPart {
	return eqPart{
		name:  name,
		value: value,
	}
}

// Range creates a query part for a range query
func Range(name string, begin Scalar, end Scalar) QueryPart {
	return rangePart{
		name:  name,
		begin: begin,
		end:   end,
	}
}

// Prefix creates a query part for a partial match
// The beginning of a value is matched against the query.
func Prefix(name string, value Scalar) QueryPart {
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
	value Scalar
}

func (e eqPart) Name() string {
	return e.name
}

func (e eqPart) Seek() Scalar {
	return e.value
}

func (e eqPart) Condition(key Key, transform Transform) bool {
	if transform != nil {
		transformed := transform(e.value)
		return bytes.Compare(key, transformed.Bytes()) == 0
	}

	return bytes.Compare(key, e.value.Bytes()) == 0
}

type rangePart struct {
	name  string
	begin Scalar
	end   Scalar
}

func (r rangePart) Name() string {
	return r.name
}

func (r rangePart) Seek() Scalar {
	return r.begin
}

func (r rangePart) Condition(key Key, transform Transform) bool {
	bTransformed := r.begin
	eTransformed := r.end
	if transform != nil {
		bTransformed = transform(r.begin)
		eTransformed = transform(r.end)
	}

	// the key becomes before the start
	if bytes.Compare(key, bTransformed.Bytes()) < 0 {
		return false
	}

	return bytes.Compare(key, eTransformed.Bytes()) <= 0
}

type prefixPart struct {
	name  string
	value Scalar
}

func (p prefixPart) Name() string {
	return p.name
}

func (p prefixPart) Seek() Scalar {
	return p.value
}

func (p prefixPart) Condition(key Key, transform Transform) bool {
	transformed := p.value
	if transform != nil {
		transformed = transform(p.value)
	}

	return bytes.HasPrefix(key, transformed.Bytes())
}
