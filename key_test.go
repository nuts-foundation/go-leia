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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComposeKey(t *testing.T) {
	t.Run("ok - empty keys", func(t *testing.T) {
		k := ComposeKey(nil, nil)

		assert.Nil(t, k)
	})

	t.Run("ok - initial key", func(t *testing.T) {
		a := Key("additional")
		k := ComposeKey(nil, a)

		assert.Equal(t, a, k)
	})

	t.Run("ok - multiple key", func(t *testing.T) {
		k1 := Key("first")
		k2 := Key("second")
		exp := Key(fmt.Sprintf("first%csecond", KeyDelimiter))

		k := ComposeKey(k1, k2)

		assert.Equal(t, exp, k)
	})

	t.Run("ok - with empty keys", func(t *testing.T) {
		k1 := Key("first")
		k2 := Key([]byte{})
		k3 := Key([]byte{})
		exp := Key(fmt.Sprintf("first%c%c", KeyDelimiter, KeyDelimiter))

		k := ComposeKey(ComposeKey(k1, k2), k3)

		assert.Equal(t, exp, k)
	})
}

func TestKey_Split(t *testing.T) {
	t.Run("ok - single key", func(t *testing.T) {
		s := Key("first").Split()

		assert.Len(t, s, 1)
	})

	t.Run("ok - multiple keys", func(t *testing.T) {
		k1 := Key("first")
		k2 := Key("second")
		c := Key(fmt.Sprintf("first%csecond", KeyDelimiter))

		s := c.Split()

		assert.Len(t, s, 2)
		assert.Equal(t, k1, s[0])
		assert.Equal(t, k2, s[1])
	})
}
