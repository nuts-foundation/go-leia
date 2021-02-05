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
	"strings"

	"github.com/thedevsaddam/gojsonq/v2"
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
	jsonq := gojsonq.New().FromString(document.String())

	if err := jsonq.Error(); err != nil {
		return nil, err
	}

	matches := j.matchR(j.pathParts(), jsonq)
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

func (j jsonIndexPart) matchR(parts []string, jsonq *gojsonq.JSONQ) []interface{} {
	jsonq = jsonq.From(parts[0])
	val := jsonq.Get()

	if val == nil {
		return []interface{}{}
	}

	if a, ok := val.([]interface{}); ok {
		if len(parts) == 1 {
			return a
		}

		var ra []interface{}
		for _, ai := range a {
			gjs := gojsonq.New().FromInterface(ai)
			interm := j.matchR(parts[1:], gjs)
			ra = append(ra, interm...)
		}

		return ra
	}

	if m, ok := val.(map[string]interface{}); ok {
		gjs := gojsonq.New().FromInterface(m)
		return j.matchR(parts[1:], gjs)
	}

	return []interface{}{val}
}
