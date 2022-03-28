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
	otherJSONPath, ok := other.(jsonPath)
	if !ok {
		return false
	}

	return q == otherJSONPath
}

// QueryPath is the interface for the query path given in queries
type QueryPath interface {
	Equals(other QueryPath) bool
}

// termPath represents a nested term structure (or graph path) using the fully qualified IRIs
type termPath struct {
	// terms represent the nested structure from highest (index 0) to lowest nesting
	terms []string
}

// NewTermPath creates a QUeryPath of JSON-LD terms
func NewTermPath(terms ...string) QueryPath {
	return termPath{terms: terms}
}

// IsEmpty returns true of no terms are in the list
func (tp termPath) IsEmpty() bool {
	return len(tp.terms) == 0
}

// Head returns the first term of the list or ""
func (tp termPath) Head() string {
	if len(tp.terms) == 0 {
		return ""
	}
	return tp.terms[0]
}

// Tail returns the last terms of the list or an empty TermPath
func (tp termPath) Tail() termPath {
	if len(tp.terms) <= 1 {
		return termPath{}
	}
	return termPath{terms: tp.terms[1:]}
}

// Equals returns true if two TermPaths have the exact same Terms in the exact same order
func (tp termPath) Equals(other QueryPath) bool {
	otherTermPath, ok := other.(termPath)
	if !ok {
		return false
	}

	if len(tp.terms) != len(otherTermPath.terms) {
		return false
	}

	for i, term := range tp.terms {
		if term != otherTermPath.terms[i] {
			return false
		}
	}
	return true
}

type QueryPart interface {
	IRIComparable
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

func (e eqPart) Equals(other IRIComparable) bool {
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

func (r rangePart) Equals(other IRIComparable) bool {
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

func (p prefixPart) Equals(other IRIComparable) bool {
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
