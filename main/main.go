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

package main

import (
	"fmt"
	"time"

	"github.com/nuts-foundation/go-leia"
)

func main() {
	var compoundIndex = leia.NewIndex("compound",
		leia.NewJSONIndexPart("id", "id"),
		leia.NewJSONIndexPart("obj", "obj.key"),
		leia.NewJSONIndexPart("list", "list.key"),
		leia.NewJSONIndexPart("sublist", "list.subList"),
	)

	s, err := leia.NewStore("./test/documents.db")
	if err != nil {
		panic(err)
	}
	c := s.Collection("json")
	if err != nil {
		panic(err)
	}
	err = c.AddIndex(compoundIndex)
	if err != nil {
		panic(err)
	}

	// populate
	//size := 32
	//for i := 0; i < size; i++ {
	//	var docs = make([]leia.Document, 0)
	//	for j := 0; j < size; j++ {
	//		for k := 0; k < size; k++ {
	//			for l := 0; l < size; l++ {
	//				docs = append(docs, genJson(i, j, k, l))
	//			}
	//		}
	//	}
	//	err = c.Add(docs)
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	//
	//fmt.Println("added docs")

	query := leia.New(leia.Eq("id", "ID16")).
		And(leia.Eq("obj", "OBJ.VAL16")).
		And(leia.Eq("list", "LIST.VAL16")).
		And(leia.Eq("sublist", "SUBLIST.VAL16"))

	t := time.Now()
	j, err := c.Find(query)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs in %s\n", len(j), time.Now().Sub(t).String())

	query2 := leia.New(leia.Range("id", "ID16", "ID17")).
		And(leia.Range("obj", "OBJ.VAL16", "OBJ.VAL17")).
		And(leia.Range("list", "LIST.VAL16", "LIST.VAL17")).
		And(leia.Range("sublist", "SUBLIST.VAL16", "SUBLIST.VAL17"))

	t = time.Now()
	j, err = c.Find(query2)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs in %s\n", len(j), time.Now().Sub(t).String())
	//fmt.Println(j[0])

	//j, err = s.Find(soObj)
	//println(err.Error())
}

// test data

var jsonTemplate1 = `
{
	"id": "%s",
	"obj": {
		"key": "%s"
	},
	"list": [
		{
			"key": "%s",
			"subList": ["%s"]
		}
	]
}
`

func genJson(i, j, k, l int) []byte {
	id := fmt.Sprintf("ID%d", i)
	key := fmt.Sprintf("OBJ.VAL%d", j)
	key2 := fmt.Sprintf("LIST.VAL%d", k)
	key3 := fmt.Sprintf("SUBLIST.VAL%d", l)

	gen := fmt.Sprintf(jsonTemplate1, id, key, key2, key3)

	return []byte(gen)
}
