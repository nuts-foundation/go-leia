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

// NewJSONIndexPart creates a new JSONIndexPart
// leave the name empty to use the json path as name.
// the name is to be used as query key when searching
func NewJSONIndexPart(name string, jsonPath string, tokenizer Tokenizer, transformer Transform) IndexPart {
	return jsonIndexPart{
		name:        name,
		jsonPath:    jsonPath,
		tokenizer:   tokenizer,
		transformer: transformer,
	}
}

type jsonIndexPart struct {
	name        string
	jsonPath    string
	tokenizer   Tokenizer
	transformer Transform
}

func (j jsonIndexPart) pathParts() []string {
	return strings.Split(j.jsonPath, ".")
}

func (j jsonIndexPart) Name() string {
	if strings.TrimSpace(j.name) == "" {
		return j.jsonPath
	}
	return j.name
}

func (j jsonIndexPart) Keys(document Document) ([]Key, error) {
	var val = make(map[string]interface{})
	if err := json.Unmarshal(document, &val); err != nil {
		return nil, errors2.Wrap(err, "unable to parse document")
	}

	matches, err := j.matchRecursive(j.pathParts(), val)
	if err != nil {
		return nil, err
	}

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

func (j jsonIndexPart) Tokenize(value interface{}) []interface{} {
	if j.tokenizer == nil {
		return []interface{}{value}
	}

	if s, ok := value.(string); ok {
		tokens := j.tokenizer(s)
		result := make([]interface{}, len(tokens))
		for i, t := range tokens {
			result[i] = t
		}
		return result
	}
	return []interface{}{value}
}

func (j jsonIndexPart) Transform(value interface{}) interface{} {
	if j.transformer == nil {
		return value
	}
	return j.transformer(value)
}

func (j jsonIndexPart) matchRecursive(parts []string, val interface{}) ([]interface{}, error) {

	if a, ok := val.([]interface{}); ok {
		var ra []interface{}
		for _, ai := range a {
			interm, err := j.matchRecursive(parts, ai)
			if err != nil {
				return nil, err
			}
			ra = append(ra, interm...)
		}

		return ra, nil
	}

	if m, ok := val.(map[string]interface{}); ok {
		if len(parts) == 0 {
			return []interface{}{}, nil
		}
		if v, ok2 := m[parts[0]]; ok2 {
			return j.matchRecursive(parts[1:], v)
		}
		// no match
		return []interface{}{}, nil
	}

	tokens := j.Tokenize(val)
	result := make([]interface{}, len(tokens))
	for i, t := range tokens {
		result[i] = j.Transform(t)
	}

	return result, nil
}
