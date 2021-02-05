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

//
//func TestNewIndex(t *testing.T) {
//	i := NewIndex("name", "path.sub")
//
//	assert.Equal(t, "name", i.Name())
//	assert.Equal(t, []string{"path", "sub"}, i.(index).parts)
//}
//
//func TestIndex_Bucket(t *testing.T) {
//	i := NewIndex("name", "path.sub")
//
//	assert.Equal(t, []byte("index_name"), i.Bucket())
//}
//
//func TestIndex_Match(t *testing.T) {
//	i := NewIndex("name", "field")
//
//	t.Run("single string value", func(t *testing.T) {
//		json := "{\"field\":\"value\"}"
//
//		matches, _ := i.Match(json)
//
//		assert.Len(t, matches, 1)
//		assert.Equal(t, "value", matches[0].(string))
//
//	})
//
//	t.Run("single number value", func(t *testing.T) {
//		json := "{\"field\":1}"
//
//		matches, _ := i.Match(json)
//
//		assert.Len(t, matches, 1)
//		assert.Equal(t, float64(1), matches[0].(float64))
//
//	})
//
//	t.Run("multiple string values", func(t *testing.T) {
//		json := "{\"field\":[\"value\", \"value\"]}"
//
//		matches, _ := i.Match(json)
//
//		assert.Len(t, matches, 2)
//		assert.Equal(t, "value", matches[0].(string))
//
//	})
//
//	t.Run("multiple number values", func(t *testing.T) {
//		json := "{\"field\":[1,0.1]}"
//
//		matches, _ := i.Match(json)
//
//		assert.Len(t, matches, 2)
//		assert.Equal(t, float64(0.1), matches[1].(float64))
//
//	})
//
//	t.Run("multiple nested string values", func(t *testing.T) {
//		i := NewIndex("name", "field.sub")
//		json := "{\"field\":[{\"sub\":\"value\"}, {\"sub\":\"value\"}]}"
//
//		matches, _ := i.Match(json)
//
//		assert.Len(t, matches, 2)
//		assert.Equal(t, "value", matches[0].(string))
//
//	})
//
//	t.Run("error - invalid json", func(t *testing.T) {
//		i := NewIndex("name", "field.sub")
//		json := "}"
//
//		_, err := i.Match(json)
//
//		assert.Error(t, err)
//	})
//}
