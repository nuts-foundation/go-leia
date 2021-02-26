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
	"encoding/json"
	"strings"

	errors2 "github.com/pkg/errors"
)


func NewJSONIndexPart(name string, jsonPath string) IndexPart {
	return jsonIndexPart{
		name: name,
		jsonPath: jsonPath,
	}
}

type jsonIndexPart struct {
	name      string
	jsonPath  string
}

func (j jsonIndexPart) pathParts() []string {
	return strings.Split(j.jsonPath, ".")
}

func (j jsonIndexPart) Name() string {
	return j.name
}

func (j jsonIndexPart) Keys(document Document) ([]Key, error) {
	var val = make(map[string]interface{})
	if err := json.Unmarshal(document, &val); err != nil {
		return nil, errors2.Wrap(err, "unable to parse document")
	}

	matches := j.matchRecursive(j.pathParts(), val)
	keys := make([]Key, len(matches))
	for i, m := range matches {
		b, err := toBytes(m)
		if err != nil {
			return nil, err
		}
		keys[i] = b
	}

	return keys, nil
}

func (j jsonIndexPart) matchRecursive(parts []string, val interface{}) []interface{} {

	if a, ok := val.([]interface{}); ok {
		var ra []interface{}
		for _, ai := range a {
			interm := j.matchRecursive(parts, ai)
			ra = append(ra, interm...)
		}

		return ra
	}

	if m, ok := val.(map[string]interface{}); ok {
		if len(parts) == 0 {
			return []interface{}{}
		}
		if v, ok2 := m[parts[0]]; ok2 {
			return j.matchRecursive(parts[1:], v)
		}
		// no match
		return []interface{}{}
	}

	return []interface{}{val}
}
