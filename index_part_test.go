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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIndexPart(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		path := NewJSONPath("path")
		ip := NewFieldIndexer(path)

		jip, ok := ip.(fieldIndexer)

		if !assert.True(t, ok) {
			return
		}
		assert.Equal(t, path, jip.QueryPath())
	})
}
