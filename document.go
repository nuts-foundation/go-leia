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

import "github.com/tidwall/gjson"

// Document represents a JSON document in []byte format
type Document struct {
	raw []byte
}

// DocumentFromString creates a Document from a JSON string
func DocumentFromString(json string) Document {
	return Document{raw:[]byte(json)}
}

// String returns the document in string format
func (d Document) String() string {
	return string(d.raw)
}

// Comparable checks if the value at the given JSON path can be compared to a single value
func (d Document) Comparable(path string) bool {
	result := gjson.GetBytes(d.raw, path)

	switch result.Type {
	case gjson.String:
		return true
	case gjson.Number:
		return true
	default:
		return false
	}
}

// GetValues returns a gjson.Result for the given JSON path
func (d Document) GetValues(pathQuery string) gjson.Result {
	return gjson.GetBytes(d.raw, pathQuery)
}

// GetString returns the string value at the given JSON path
func (d Document) GetString(pathQuery string) string {
	result := gjson.GetBytes(d.raw, pathQuery)

	return result.String()
}

// GetNumber returns a float64 of the number at the given JSON path
func (d Document) GetNumber(pathQuery string) float64 {
	result := gjson.GetBytes(d.raw, pathQuery)

	return result.Float()
}

