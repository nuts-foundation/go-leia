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

//
//func TestNewStore(t *testing.T) {
//	i := NewIndex("index", "path")
//	dir := testDirectory(t)
//
//	s, err := NewStore(dir, i)
//
//	if !assert.NoError(t, err) {
//		return
//	}
//
//	assert.NotNil(t, s)
//}
//
//var json = []byte(`
//{
//	"key": "value",
//	"other": "some other value"
//}
//`)
//
//var json2 = []byte(`
//{
//	"key": "value2"
//}
//`)
//
//var json3 = []byte(`
//{
//	"key": "value",
//	"other": "also a value"
//}
//`)
//
//func TestStore_Add(t *testing.T) {
//	t.Run("ok - single doc in i ndex", func(t *testing.T) {
//		dir := testDirectory(t)
//		s, _ := NewStore(dir)
//
//		err := s.Add([]Document{json})
//
//		assert.NoError(t, err)
//	})
//
//	t.Run("ok - multi doc in index", func(t *testing.T) {
//		dir := testDirectory(t)
//		s, _ := NewStore(dir)
//
//		err := s.Add([]Document{json, json3})
//
//		assert.NoError(t, err)
//	})
//
//	t.Run("error - invalid json when index is used", func(t *testing.T) {
//		dir := testDirectory(t)
//		s, _ := NewStore(dir, NewIndex("index", "key"))
//
//		err := s.Add([]Document{[]byte("{")})
//
//		assert.Error(t, err)
//	})
//}
//
//func TestStore_Get(t *testing.T) {
//	dir := testDirectory(t)
//	s, _ := NewStore(dir)
//	s.Add([]Document{json})
//	ref := NewReference(json)
//
//	t.Run("ok", func(t *testing.T) {
//		d, err := s.Get(ref)
//
//		if !assert.NoError(t, err) {
//			return
//		}
//
//		assert.Equal(t, json, d)
//	})
//
//	t.Run("not found", func(t *testing.T) {
//		d, err := s.Get(NewReference([]byte("unknown")))
//
//		if !assert.NoError(t, err) {
//			return
//		}
//
//		assert.Nil(t, d)
//	})
//}
//
//func TestStore_Find(t *testing.T) {
//	i := NewIndex("index", "key")
//	dir := testDirectory(t)
//	s, _ := NewStore(dir, i)
//	s.Add([]Document{json, json2, json3})
//
//	t.Run("ok - single result", func(t *testing.T) {
//		d, err := s.Find(StringSearchOption{
//			index: "index",
//			value: "value2",
//		})
//
//		if !assert.NoError(t, err) {
//			return
//		}
//
//		assert.Len(t, d, 1)
//		assert.Equal(t, json2, []byte(d[0]))
//	})
//
//	t.Run("ok - multi result", func(t *testing.T) {
//		d, err := s.Find(StringSearchOption{
//			index: "index",
//			value: "value",
//		})
//
//		if !assert.NoError(t, err) {
//			return
//		}
//
//		assert.Len(t, d, 2)
//	})
//
//	t.Run("not found", func(t *testing.T) {
//		d, err := s.Find(StringSearchOption{
//			index: "index",
//			value: "unknown",
//		})
//
//		if !assert.NoError(t, err) {
//			return
//		}
//
//		assert.Len(t, d, 0)
//	})
//}
//
//func TestStore_Delete(t *testing.T) {
//	i := NewIndex("index", "key")
//	dir := testDirectory(t)
//	s, _ := NewStore(dir, i)
//	s.Add([]Document{json, json2, json3})
//	ref := NewReference(json2)
//
//	t.Run("ok", func(t *testing.T) {
//		err := s.Delete(json2)
//
//		if !assert.NoError(t, err) {
//			return
//		}
//
//		d, _ := s.Get(ref)
//
//		assert.Nil(t, d)
//	})
//
//	t.Run("ok - non-existing", func(t *testing.T) {
//		err := s.Delete([]byte("{}"))
//
//		assert.NoError(t, err)
//	})
//
//	t.Run("ok - part of multiple", func(t *testing.T) {
//		err := s.Delete(json)
//
//		if !assert.NoError(t, err) {
//			return
//		}
//
//		d, err := s.Find(StringSearchOption{
//			index: "index",
//			value: "value",
//		})
//
//		if !assert.NoError(t, err) {
//			return
//		}
//
//		assert.Len(t, d, 1)
//	})
//
//	t.Run("error - invalid json when index is used", func(t *testing.T) {
//		err := s.Delete([]byte("{"))
//
//		assert.Error(t, err)
//	})
//}
//
//func testDirectory(t *testing.T) string {
//	if dir, err := ioutil.TempDir("", normalizeTestName(t)); err != nil {
//		t.Fatal(err)
//		return ""
//	} else {
//		t.Cleanup(func() {
//			if err := os.RemoveAll(dir); err != nil {
//				_, _ = os.Stderr.WriteString(fmt.Sprintf("Unable to remove temporary directory for test (%s): %v\n", dir, err))
//			}
//		})
//		return dir
//	}
//}
//
//func normalizeTestName(t *testing.T) string {
//	var invalidPathCharRegex = regexp.MustCompile("([^a-zA-Z0-9])")
//	return invalidPathCharRegex.ReplaceAllString(t.Name(), "_")
//}
