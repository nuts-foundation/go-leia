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

var testAsScalar = MustParseScalar("test")
var testJsonPath = NewJSONPath("test")

func TestNew(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		q := New(Eq(testJsonPath, testAsScalar))

		assert.Len(t, q.parts, 1)
	})
}

func TestQuery_And(t *testing.T) {
	q := New(Eq(testJsonPath, testAsScalar))

	t.Run("ok", func(t *testing.T) {
		q = q.And(Eq(testJsonPath, testAsScalar))

		assert.Len(t, q.parts, 2)
	})
}

func TestEq(t *testing.T) {
	qp := Eq(testJsonPath, testAsScalar)

	t.Run("ok - seek", func(t *testing.T) {
		s := qp.Seek()

		assert.Equal(t, "test", s.value())
	})

	t.Run("ok - condition true", func(t *testing.T) {
		c := qp.Condition(Key("test"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition false", func(t *testing.T) {
		c := qp.Condition(Key("test2"), nil)

		assert.False(t, c)
	})
}

func TestRange_Condition(t *testing.T) {
	qp := Range(testJsonPath, MustParseScalar("a"), MustParseScalar("b"))

	t.Run("ok - seek", func(t *testing.T) {
		s := qp.Seek()

		assert.Equal(t, "a", s.value())
	})

	t.Run("ok - condition true begin", func(t *testing.T) {
		c := qp.Condition(Key("a"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition true middle", func(t *testing.T) {
		c := qp.Condition(Key("ab"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition true end", func(t *testing.T) {
		c := qp.Condition(Key("b"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition false", func(t *testing.T) {
		c := qp.Condition(Key("bb"), nil)

		assert.False(t, c)
	})

	t.Run("ok - with transform", func(t *testing.T) {
		qp := Range(testJsonPath, MustParseScalar("A"), MustParseScalar("B"))

		c := qp.Condition(Key("a"), ToLower)

		assert.True(t, c)
	})
}

func TestPrefixPart_Condition(t *testing.T) {
	qp := Prefix(testJsonPath, testAsScalar)

	t.Run("ok - seek", func(t *testing.T) {
		s := qp.Seek()

		assert.Equal(t, "test", s.value())
	})

	t.Run("ok - condition true", func(t *testing.T) {
		c := qp.Condition(Key("test something"), nil)

		assert.True(t, c)
	})

	t.Run("ok - condition true with transform", func(t *testing.T) {
		qp := Prefix(testJsonPath, MustParseScalar("TEST"))

		c := qp.Condition(Key("test something"), ToLower)

		assert.True(t, c)
	})

	t.Run("ok - condition false", func(t *testing.T) {
		c := qp.Condition(Key("is not test"), nil)

		assert.False(t, c)
	})

	t.Run("ok - key too short", func(t *testing.T) {
		c := qp.Condition(Key("te"), nil)

		assert.False(t, c)
	})
}

func TestPrefixPart_Equals(t *testing.T) {
	qp := Prefix(testJsonPath, testAsScalar)

	t.Run("true", func(t *testing.T) {
		assert.True(t, qp.Equals(qp))
	})

	t.Run("false", func(t *testing.T) {
		assert.False(t, qp.Equals(Prefix(NewJSONPath("a"), MustParseScalar("a"))))
	})
}

func TestRangePart_Equals(t *testing.T) {
	qp := Range(testJsonPath, MustParseScalar("a"), MustParseScalar("b"))

	t.Run("true", func(t *testing.T) {
		assert.True(t, qp.Equals(qp))
	})

	t.Run("false", func(t *testing.T) {
		assert.False(t, qp.Equals(Range(NewJSONPath("a"), MustParseScalar("a"), MustParseScalar("b"))))
	})
}

func TestTermPath_Equals(t *testing.T) {
	t.Run("false - for other type of QueryPath", func(t *testing.T) {
		assert.False(t, NewJSONPath(".").Equals(NewIRIPath()))
	})

	t.Run("false - different number of terms", func(t *testing.T) {
		assert.False(t, NewIRIPath("1").Equals(NewIRIPath("1", "2")))
	})

	t.Run("false - different terms", func(t *testing.T) {
		assert.False(t, NewIRIPath("1").Equals(NewIRIPath("a")))
	})

	t.Run("true", func(t *testing.T) {
		assert.True(t, NewIRIPath("1").Equals(NewIRIPath("1")))
	})
}

func TestJSONPath_Equals(t *testing.T) {
	assert.False(t, NewIRIPath().Equals(NewJSONPath(".")))
}

func TestNotNilPart_Seek(t *testing.T) {
	assert.Equal(t, []byte{0}, NotNil(testJsonPath).Seek().value())
}

func TestNotNilPart_Condition(t *testing.T) {
	assert.True(t, NotNil(testJsonPath).Condition([]byte{0}, nil))
	assert.False(t, NotNil(testJsonPath).Condition([]byte{}, nil))
}

func TestNotNilPart_Equals(t *testing.T) {
	qp := NotNil(testJsonPath)

	t.Run("true", func(t *testing.T) {
		assert.True(t, qp.Equals(qp))
	})

	t.Run("false", func(t *testing.T) {
		assert.False(t, qp.Equals(NotNil(NewJSONPath("a"))))
	})
}
