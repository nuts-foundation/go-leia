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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/nuts-foundation/go-leia/v3"
)

func main() {
	dir, err := ioutil.TempDir("", "options")
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
	c := s.Collection(leia.JSONCollection, "json")
	var compoundIndex = c.NewIndex("compound",
		leia.NewFieldIndexer(leia.NewJSONPath("id"), leia.TokenizerOption(leia.WhiteSpaceTokenizer), leia.TransformerOption(leia.ToLower)),
		leia.NewFieldIndexer(leia.NewJSONPath("obj.key"), leia.TokenizerOption(leia.WhiteSpaceTokenizer)),
		leia.NewFieldIndexer(leia.NewJSONPath("list.#.key"), leia.TokenizerOption(leia.WhiteSpaceTokenizer)),
		leia.NewFieldIndexer(leia.NewJSONPath("list.#.subList"), leia.TokenizerOption(leia.WhiteSpaceTokenizer)),
	)
	err = c.AddIndex(compoundIndex)
	if err != nil {
		panic(err)
	}

	// populate
	size := 16
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
	query := leia.New(leia.Eq(leia.NewJSONPath("id"), leia.MustParseScalar("id13"))).
		And(leia.Eq(leia.NewJSONPath("obj.key"), leia.MustParseScalar("OBJ.VAL13"))).
		And(leia.Eq(leia.NewJSONPath("list.#.key"), leia.MustParseScalar("LIST.VAL13"))).
		And(leia.Eq(leia.NewJSONPath("list.#.subList"), leia.MustParseScalar("SUBLIST.VAL13")))

	j, err := c.Find(context.Background(), query)
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
	query2 := leia.New(leia.Range(leia.NewJSONPath("id"), leia.MustParseScalar("ID13"), leia.MustParseScalar("ID14"))).
		And(leia.Range(leia.NewJSONPath("obj.key"), leia.MustParseScalar("OBJ.VAL13"), leia.MustParseScalar("OBJ.VAL14"))).
		And(leia.Range(leia.NewJSONPath("list.#.key"), leia.MustParseScalar("LIST.VAL13"), leia.MustParseScalar("LIST.VAL14"))).
		And(leia.Range(leia.NewJSONPath("sublist.#.subList"), leia.MustParseScalar("SUBLIST.VAL13"), leia.MustParseScalar("SUBLIST.VAL14")))

	j, err = c.Find(context.Background(), query2)
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
	query3 := leia.New(leia.Range(leia.NewJSONPath("list.#.subList"), leia.MustParseScalar("SUBLIST.VAL13"), leia.MustParseScalar("SUBLIST.VAL14"))).
		And(leia.Range(leia.NewJSONPath("list.#.key"), leia.MustParseScalar("LIST.VAL13"), leia.MustParseScalar("LIST.VAL14")))

	j, err = c.Find(context.Background(), query3)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs\n", len(j))

	// combination of an index and additional constraints
	query4 := leia.New(leia.Range(leia.NewJSONPath("id"), leia.MustParseScalar("ID13"), leia.MustParseScalar("ID14"))).
		And(leia.Range(leia.NewJSONPath("list.#.subList"), leia.MustParseScalar("SUBLIST.VAL13"), leia.MustParseScalar("SUBLIST.VAL14"))).
		And(leia.Range(leia.NewJSONPath("list.#.key"), leia.MustParseScalar("LIST.VAL13"), leia.MustParseScalar("LIST.VAL14")))

	j, err = c.Find(context.Background(), query4)
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

	return []byte(gen)
}
