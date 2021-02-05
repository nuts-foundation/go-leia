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

type Collection interface {
	AddIndex(index Index) error
	DropIndex(name string) error
	Indices() []Index

	Add(doc Document) error
	Get(ref Reference) (Document, error)
	Delete(ref Reference) error

	Find(query Query) ([]Reference, error)

	Reference(doc Document) (Reference, error)
}

type KeyExtractor interface {
	// Key checks if the document can be indexed and returns the Key if so, nil otherwise
	Key(document Document) ([]Key, error)
}
