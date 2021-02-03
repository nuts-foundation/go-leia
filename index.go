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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/thedevsaddam/gojsonq/v2"
	"go.etcd.io/bbolt"
)

// Index describes an index. An index is based on a json path and has a name.
// The name is used for storage but also as identifier in search options.
type Index interface {
	// Name returns the name of this index.
	Name() string

	// Bucket returns the bucket identifier where index entries are stored
	Bucket() []byte

	// Match returns the matches found in a JSON document. An error is returned when the json isn't valid.
	Match(json string) ([]interface{}, error)

	// AddIfMatch adds a reference of a document to the index if the index path matches.
	AddIfMatch(tx *bbolt.Tx, doc Document, ref Reference) error

	// DeleteIfMatch removes a reference of a document form the index.
	DeleteIfMatch(tx *bbolt.Tx, doc Document, ref Reference) error
}

// NewIndex creates a new index with name and given JSON search path.
func NewIndex(name string, jsonPath string) Index {
	i := index{
		name: name,
		jsonPath: jsonPath,
	}
	return i.withParts()
}

type index struct {
	name string
	jsonPath string
	parts []string
}

func (i index) Name() string {
	return i.name
}

func (i index) Bucket() []byte {
	s := fmt.Sprintf("index_%s", i.name)
	return []byte(s)
}

func (i index) Match(json string) (matches []interface{}, err error) {
	jsonq := gojsonq.New().FromString(json)

	if err = jsonq.Error(); err != nil {
		return
	}

	matches = i.match(i.parts, jsonq)

	return
}

func (i index) match(parts []string, jsonq *gojsonq.JSONQ) []interface{} {
	jsonq = jsonq.From(parts[0])
	val := jsonq.Get()

	if a, ok := val.([]interface{}); ok {
		if len(parts) == 1 {
			return a
		}

		var ra []interface{}
		for _, ai := range a {
			gjs := gojsonq.New().FromInterface(ai)
			interm := i.match(parts[1:], gjs)
			ra = append(ra, interm...)
		}

		return ra
	}

	if v, ok := val.(string); ok {
		return []interface{}{v}
	}

	if v, ok := val.(float64); ok {
		return []interface{}{v}
	}

	if m, ok := val.(map[string]interface{}); ok {
		gjs := gojsonq.New().FromInterface(m)
		return i.match(parts[1:], gjs)
	}

	return []interface{}{}
}

func (i index) copy() Index {
	return index{
		name:     i.name,
		jsonPath: i.jsonPath,
		parts:    i.parts,
	}
}

func (i index) withParts() Index {
	parts := strings.Split(i.jsonPath, ".")
	for _, p := range parts {
		i.parts = append(i.parts, p)
	}

	return i.copy()
}


func (i index) AddIfMatch(tx *bbolt.Tx, doc Document, ref Reference) error {
	iBucket := tx.Bucket(i.Bucket())
	val, err := i.Match(string(doc))
	if err != nil {
		return err
	}

	for _, key := range val {
		b, err := toBytes(key)
		if err != nil {
			return err
		}

		entryBytes := iBucket.Get(b)
		var entry Entry

		if len(entryBytes) == 0 {
			entry = EntryFrom(ref)
		} else {
			if err := entry.Unmarshal(entryBytes); err != nil {
				return err
			}
			entry.Add(ref)
		}

		iBytes, err := entry.Marshal()
		if err != nil {
			return err
		}

		if err := iBucket.Put(b, iBytes); err != nil {
			return err
		}
	}

	return nil
}


func (i index) DeleteIfMatch(tx *bbolt.Tx, doc Document, ref Reference) error {
	iBucket := tx.Bucket(i.Bucket())
	val, err := i.Match(string(doc))
	if err != nil {
		return err
	}

	for _, key := range val {
		b, err := toBytes(key)
		if err != nil {
			return err
		}

		entryBytes := iBucket.Get(b)
		var entry Entry

		if len(entryBytes) == 0 {
			continue
		}

		if err := entry.Unmarshal(entryBytes); err != nil {
			return err
		}
		entry.Delete(ref)

		if entry.Size() > 0 {
			iBytes, err := entry.Marshal()
			if err != nil {
				return err
			}
			if err := iBucket.Put(b, iBytes); err != nil {
				return err
			}
		} else {
			if err := iBucket.Delete(b); err != nil {
				return err
			}
		}
	}

	return nil
}

func toBytes(data interface{}) ([]byte, error) {
	if s, ok := data.(string); ok {
		return []byte(s), nil
	}
	if f, ok := data.(float64); ok {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
		return buf[:], nil
	}
	return nil, errors.New("couldn't convert data to []byte")
}