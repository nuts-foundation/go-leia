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
	"io/ioutil"
	"os"
	"path"

	"github.com/nuts-foundation/go-leia"
)

func main() {
	var compoundIndex = leia.NewIndex("compound",
		leia.NewFieldIndexer("id", leia.TokenizerOption(leia.WhiteSpaceTokenizer), leia.TransformerOption(leia.ToLower)),
		leia.NewFieldIndexer("obj.key", leia.AliasOption("obj"), leia.TokenizerOption(leia.WhiteSpaceTokenizer)),
		leia.NewFieldIndexer("list.#.key", leia.AliasOption("list"), leia.TokenizerOption(leia.WhiteSpaceTokenizer)),
		leia.NewFieldIndexer("list.#.subList", leia.AliasOption("sublist"), leia.TokenizerOption(leia.WhiteSpaceTokenizer)),
	)

	dir, err := ioutil.TempDir("go-leia", "options")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			_, _ = os.Stderr.WriteString(fmt.Sprintf("Unable to remove temporary directory (%s): %v\n", dir, err))
		}
	}()

	s, err := leia.NewStore(path.Join(dir, "documents.db"))
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
	size := 32
	for i := 0; i < size; i++ {
		var docs = make([]leia.Document, 0)
		for j := 0; j < size; j++ {
			for k := 0; k < size; k++ {
				for l := 0; l < size; l++ {
					docs = append(docs, genJson(i, j, k, l))
				}
			}
		}
		err = c.Add(docs)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("added docs")

	// only matches when toLower is working properly
	query := leia.New(leia.Eq("id", "id16")).
		And(leia.Eq("obj", "OBJ.VAL16")).
		And(leia.Eq("list", "LIST.VAL16")).
		And(leia.Eq("sublist", "SUBLIST.VAL16"))

	j, err := c.Find(query)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs\n", len(j))
	i := 0
	c.IndexIterate(query, func(key []byte, value []byte) error {
		i++
		return nil
	})
	fmt.Printf("found %d keys\n", i)

	// only matches when range queries are working properly
	query2 := leia.New(leia.Range("id", "ID16", "ID17")).
		And(leia.Range("obj", "OBJ.VAL16", "OBJ.VAL17")).
		And(leia.Range("list", "LIST.VAL16", "LIST.VAL17")).
		And(leia.Range("sublist", "SUBLIST.VAL16", "SUBLIST.VAL17"))

	j, err = c.Find(query2)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs\n", len(j))
	i = 0

	c.IndexIterate(query2, func(key []byte, value []byte) error {
		i++
		return nil
	})
	fmt.Printf("found %d keys\n", i)

	// only matches when full table scan is working properly
	query3 := leia.New(leia.Range("list.#.subList", "SUBLIST.VAL16", "SUBLIST.VAL17")).
		And(leia.Range("list.#.key", "LIST.VAL16", "LIST.VAL17"))

	j, err = c.Find(query3)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs\n", len(j))

	// combination of an index and additional constraints
	query4 := leia.New(leia.Range("id", "ID16", "ID17")).
		And(leia.Range("list.#.subList", "SUBLIST.VAL16", "SUBLIST.VAL17")).
		And(leia.Range("list.#.key", "LIST.VAL16", "LIST.VAL17"))

	j, err = c.Find(query4)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs\n", len(j))
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

func genJson(i, j, k, l int) leia.Document {
	id := fmt.Sprintf("ID%d", i)
	key := fmt.Sprintf("OBJ.VAL%d", j)
	key2 := fmt.Sprintf("LIST.VAL%d", k)
	key3 := fmt.Sprintf("SUBLIST.VAL%d", l)

	gen := fmt.Sprintf(jsonTemplate1, id, key, key2, key3)

	return leia.DocumentFromString(gen)
}
