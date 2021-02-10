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
	"fmt"
	"strconv"
)

// Entry holds references to documents from specific values that can be used in a search.
// It requires references to be of equal size
type Entry struct {
	// RefSize defines the number of bytes per reference
	RefSize int

	// references simulates a set, keys are hex encoded entries of Reference
	references map[string]Reference
}

// EntryFrom creates an Entry from a reference. The RefSize is based on the number of bytes given
func EntryFrom(ref Reference) Entry {
	references := make(map[string]Reference)
	references[ref.EncodeToString()] = ref

	return Entry{
		RefSize:    ref.ByteSize(),
		references: references,
	}
}

// Add a reference to the current entry
func (e *Entry) Add(ref Reference) error {
	if len(ref) != e.RefSize {
		return fmt.Errorf("given reference is of different size, given %d, required: %d", len(ref), e.RefSize)
	}

	e.references[ref.EncodeToString()] = ref

	return nil
}

// Delete removes a reference from this entry
func (e *Entry) Delete(ref Reference) {
	delete(e.references, ref.EncodeToString())
}

// Slice returns a slice of References.
func (e *Entry) Slice() []Reference {
	refs := make([]Reference, len(e.references))

	i := 0
	for _, v := range e.references {
		refs[i] = v
		i++
	}
	return refs
}

// Marshal an entry to byte form. The Byte size is marshalled as string followed by a '#', then all references are marshalled as bytes.
func (e Entry) Marshal() ([]byte, error) {
	buf := bytes.NewBufferString(fmt.Sprintf("%d#", e.RefSize))
	for _, ref := range e.Slice() {
		if _, err := buf.Write(ref); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// Unmarshal an entry from binary form
func (e *Entry) Unmarshal(bytes []byte) error {
	var sizeString []byte
	var refBytes []byte

	for i, b := range bytes {
		if string(b) == "#" {
			refSize, err := strconv.ParseInt(string(sizeString), 10, 0)
			if err != nil {
				return err
			}

			refBytes = make([]byte, len(bytes)-i-1)
			e.RefSize = int(refSize)
			copy(refBytes, bytes[i+1:])

			break
		}
		sizeString = append(sizeString, b)
	}

	// copy refs from refBytes
	e.references = make(map[string]Reference)
	for i := 0; i < len(refBytes); i += e.RefSize {
		var ref = make([]byte, e.RefSize)
		copy(ref, refBytes[i:i+e.RefSize])
		if err := e.Add(ref); err != nil {
			return err
		}
	}

	return nil
}

// Size returns the number of entries in the entry
func (e *Entry) Size() int {
	return len(e.references)
}
