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

func TestNew(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		q := New(Eq("test", "test"))

		assert.Len(t, q.Parts(), 1)
	})
}

func TestQuery_And(t *testing.T) {
	q := New(Eq("test", "test"))

	t.Run("ok", func(t *testing.T) {
		q = q.And(Eq("test", "test"))

		assert.Len(t, q.Parts(), 2)
	})
}

func TestEq(t *testing.T) {
	qp := Eq("test", "test")

	t.Run("ok - name", func(t *testing.T) {
		assert.Equal(t, "test", qp.Name())
	})

	t.Run("ok - seek", func(t *testing.T) {
		s, err := qp.Seek()

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, "test", s.String())
	})

	t.Run("ok - condition true", func(t *testing.T) {
		c, err := qp.Condition(Key("test"), nil)

		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, c)
	})

	t.Run("ok - condition false", func(t *testing.T) {
		c, err := qp.Condition(Key("test2"), nil)

		if !assert.NoError(t, err) {
			return
		}

		assert.False(t, c)
	})

	t.Run("error - wrong type", func(t *testing.T) {
		qp := Eq("test", struct{}{})

		_, err := qp.Condition(Key{}, nil)

		assert.Error(t, err)
	})
}

func TestRange(t *testing.T) {
	qp := Range("test", "a", "b")

	t.Run("ok - name", func(t *testing.T) {
		assert.Equal(t, "test", qp.Name())
	})

	t.Run("ok - seek", func(t *testing.T) {
		s, err := qp.Seek()

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, "a", s.String())
	})

	t.Run("ok - condition true begin", func(t *testing.T) {
		c, err := qp.Condition(Key("a"), nil)

		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, c)
	})

	t.Run("ok - condition true middle", func(t *testing.T) {
		c, err := qp.Condition(Key("ab"), nil)

		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, c)
	})

	t.Run("ok - condition true end", func(t *testing.T) {
		c, err := qp.Condition(Key("b"), nil)

		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, c)
	})

	t.Run("ok - condition false", func(t *testing.T) {
		c, err := qp.Condition(Key("bb"), nil)

		if !assert.NoError(t, err) {
			return
		}

		assert.False(t, c)
	})

	t.Run("error - wrong type", func(t *testing.T) {
		qp := Range("test", "a", struct{}{})

		_, err := qp.Condition(Key{}, nil)

		assert.Error(t, err)
	})
}

func TestPrefix(t *testing.T) {
	qp := Prefix("test", "test")

	t.Run("ok - name", func(t *testing.T) {
		assert.Equal(t, "test", qp.Name())
	})

	t.Run("ok - seek", func(t *testing.T) {
		s, err := qp.Seek()

		if !assert.NoError(t, err) {
			return
		}

		assert.Equal(t, "test", s.String())
	})

	t.Run("ok - condition true", func(t *testing.T) {
		c, err := qp.Condition(Key("test something"), nil)

		if !assert.NoError(t, err) {
			return
		}

		assert.True(t, c)
	})

	t.Run("ok - condition false", func(t *testing.T) {
		c, err := qp.Condition(Key("is not test"), nil)

		if !assert.NoError(t, err) {
			return
		}

		assert.False(t, c)
	})

	t.Run("ok - key too short", func(t *testing.T) {
		c, err := qp.Condition(Key("te"), nil)

		if !assert.NoError(t, err) {
			return
		}

		assert.False(t, c)
	})

	t.Run("error - wrong type", func(t *testing.T) {
		qp := Eq("test", struct{}{})

		_, err := qp.Condition(Key{}, nil)

		assert.Error(t, err)
	})
}
