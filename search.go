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

type jsonPath string

// NewJSONPath creates a JSON path query: "person.path" or "person.children.#.path"
// # is used to traverse arrays
func NewJSONPath(path string) QueryPath {
	return jsonPath(path)
}

func (q jsonPath) Equals(other QueryPath) bool {
	return q == other
}

// QueryPath is the interface for the query path given in queries
type QueryPath interface {
	Equals(other QueryPath) bool
}

// iriPath represents a nested structure (or graph path) using the fully qualified IRIs
type iriPath struct {
	// iris represent the nested structure from highest (index 0) to lowest
	iris []string
}

// NewIRIPath creates a QueryPath of JSON-LD terms
func NewIRIPath(IRIs ...string) QueryPath {
	return iriPath{iris: IRIs}
}

// IsEmpty returns true of no terms are in the list
func (tp iriPath) IsEmpty() bool {
	return len(tp.iris) == 0
}

// Head returns the first IRI of the list or ""
func (tp iriPath) Head() string {
	if len(tp.iris) == 0 {
		return ""
	}
	return tp.iris[0]
}

// Tail returns the last terms of the list or an empty TermPath
func (tp iriPath) Tail() iriPath {
	if len(tp.iris) <= 1 {
		return iriPath{}
	}
	return iriPath{iris: tp.iris[1:]}
}

// Equals returns true if two TermPaths have the exact same Terms in the exact same order
func (tp iriPath) Equals(other QueryPath) bool {
	otherIRIPath, ok := other.(iriPath)
	if !ok {
		return false
	}

	if len(tp.iris) != len(otherIRIPath.iris) {
		return false
	}

	for i, iri := range tp.iris {
		if iri != otherIRIPath.iris[i] {
			return false
		}
	}
	return true
}

type QueryPart interface {
	QueryPathComparable
	// Seek returns the key for cursor.Seek
	Seek() Scalar
	// Condition returns true if given key falls within this condition.
	// The optional transform fn is applied to this query part before evaluation is done.
	Condition(key Key, transform Transform) bool
}

// New creates a new query with an initial query part. Both begin and end are inclusive for the conditional check.
func New(part QueryPart) Query {
	return Query{
		parts: []QueryPart{part},
	}
}

// Eq creates a query part for an exact match
func Eq(queryPath QueryPath, value Scalar) QueryPart {
	return eqPart{
		queryPath: queryPath,
		value:     value,
	}
}

// Range creates a query part for a range query
func Range(queryPath QueryPath, begin Scalar, end Scalar) QueryPart {
	return rangePart{
		queryPath: queryPath,
		begin:     begin,
		end:       end,
	}
}

// NotNil creates a query part where the value must exist.
// This is done by finding results between byte 0x0 and 0xff
func NotNil(queryPath QueryPath) QueryPart {
	return notNilPart{
		queryPath: queryPath,
	}
}

// Prefix creates a query part for a partial match
// The beginning of a value is matched against the query.
func Prefix(queryPath QueryPath, value Scalar) QueryPart {
	return prefixPart{
		queryPath: queryPath,
		value:     value,
	}
}

// Query represents a query with multiple arguments
type Query struct {
	parts []QueryPart
}

func (q Query) And(part QueryPart) Query {
	q.parts = append(q.parts, part)
	return q
}

type eqPart struct {
	queryPath QueryPath
	value     Scalar
}

func (e eqPart) Equals(other QueryPathComparable) bool {
	return e.queryPath.Equals(other.QueryPath())
}

func (e eqPart) QueryPath() QueryPath {
	return e.queryPath
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
	queryPath QueryPath
	begin     Scalar
	end       Scalar
}

func (r rangePart) Equals(other QueryPathComparable) bool {
	return r.queryPath.Equals(other.QueryPath())
}

func (r rangePart) QueryPath() QueryPath {
	return r.queryPath
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
	queryPath QueryPath
	value     Scalar
}

func (p prefixPart) Equals(other QueryPathComparable) bool {
	return p.queryPath.Equals(other.QueryPath())
}

func (p prefixPart) QueryPath() QueryPath {
	return p.queryPath
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

type notNilPart struct {
	queryPath QueryPath
}

func (p notNilPart) Equals(other QueryPathComparable) bool {
	return p.queryPath.Equals(other.QueryPath())
}

func (p notNilPart) QueryPath() QueryPath {
	return p.queryPath
}

func (p notNilPart) Seek() Scalar {
	return bytesScalar{}
}

func (p notNilPart) Condition(key Key, _ Transform) bool {
	return len(key) > 0
}
