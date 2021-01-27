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
	"fmt"
	"time"
)

func main() {
	s, err := NewStore("./test/", indexID, indexInt, indexObj, indexList, indexList2)
	//s, err := NewStore("./test/", indexList)
	if err != nil {
		panic(err)
	}

	// populate
	var l = make([]Document, 1000)
	for i := 0; i < 1000;i++ {
		l[i] = genJson(i)
	}

	err = s.Add(l)
	if err != nil {
		panic(err)
	}
	fmt.Println("added docs")

	//j, err := s.Find(SearchOption{
	//	Index: "id",
	//	Value: "ID56",
	//})
	//if err != nil {
	//	panic(err)
	//}
	////fmt.Printf("found doc: %s", string(j))
	//
	//j, err = s.Find(SearchOption{
	//	Index: "obj",
	//	Value: "OBJ.VAL56",
	//})
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("found doc: %s", string(j))

	t := time.Now()

	j, err := s.Find(FloatSearchOption{
		index: "intId",
		value: 10,
	})
	if err != nil {
		panic(err)
	}

	st := time.Now().Sub(t)
	fmt.Printf("took: %s", st.String())

	for _, d := range j {
		fmt.Printf("found doc: %s", string(d))
		s.Delete(d)
	}
	j, err = s.Find(FloatSearchOption{
		index: "intId",
		value: 10,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs\n", len(j))

	st = time.Now().Sub(t)
	fmt.Printf("took: %s", st.String())
}

// test data

var jsonTemplate1 = `
{
	"id": "%s",
	"intId": %d,
	"obj": {
		"key": "%s"
	},
	"list": [
		{
			"key": "%s"
		},
		{
			"key": "%s"
		}
	],
	"list2": ["%s", "%s"]
}
`
var indexID, _ = NewIndex("id","id")
var indexInt, _ = NewIndex("intId","intId")
var indexObj, _ = NewIndex("obj","obj.key")
var indexList, _ = NewIndex("list","list.key")
var indexList2, _ = NewIndex("list2","list2")

func genJson(i int) []byte {
	id := fmt.Sprintf("ID%d", i)
	key := fmt.Sprintf("OBJ.VAL%d", i)
	key2 := fmt.Sprintf("LIST.VAL%d", i)
	key3 := fmt.Sprintf("LIST.VAL%d", i+1)

	gen := fmt.Sprintf(jsonTemplate1, id, i, key, key2, key3, key2, key3)

	return []byte(gen)
}
