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
	"errors"
	"fmt"

	"github.com/tidwall/gjson"
)

// Document represents a JSON document in []byte format
type Document struct {
	raw []byte
}

// DocumentFromString creates a Document from a JSON string
func DocumentFromString(json string) Document {
	return Document{raw:[]byte(json)}
}

// DocumentFromBytes creates a Document from a JSON string
func DocumentFromBytes(json []byte) Document {
	return Document{raw: json}
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

func (d Document) KeysAtPath(jsonPath string) ([]Key, error) {
	if !gjson.ValidBytes(d.raw) {
		return nil, errors.New("invalid json")
	}
	result := gjson.GetBytes(d.raw, jsonPath)

	rawKeys, err := valuesFromResult(result)
	if err != nil {
		return nil, err
	}

	keys := make([]Key, len(rawKeys))
	for i, rk := range rawKeys {
		key, err := toBytes(rk)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	return keys, nil
}

func (d Document) ValuesAtPath(jsonPath string) ([]interface{}, error) {
	if !gjson.ValidBytes(d.raw) {
		return nil, errors.New("invalid json")
	}
	result := gjson.GetBytes(d.raw, jsonPath)

	return valuesFromResult(result)
}

func valuesFromResult(result gjson.Result) ([]interface{}, error) {
	switch result.Type {
	case gjson.String:
		return []interface{}{result.Str}, nil
	case gjson.Number:
		return []interface{}{result.Num}, nil
	case gjson.Null:
		return []interface{}{}, nil
	default:
		if result.IsArray() {
			keys := make([]interface{}, 0)
			for _, subResult := range result.Array() {
				subKeys, err := valuesFromResult(subResult)
				if err != nil {
					return nil, err
				}
				keys = append(keys, subKeys...)
			}
			return keys, nil
		}
	}
	return nil, fmt.Errorf("type at path not supported for indexing: %s", result.String())
}
