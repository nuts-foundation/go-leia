/*
 * go-leia
 * Copyright (C) 2022 Nuts community
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

	"github.com/nuts-foundation/go-leia/v2"
)

var personTemplate = `
{
  "@context": {
    "id": "@id",
    "type": "@type",
    "schema": "http://example.com/",
    "Person": {
      "@id": "schema:Person",
      "@context": {
        "id": "@id",
        "type": "@type",
        
        "name": {"@id": "schema:name"},
        "telephone": {"@id": "schema:telephone"},
        "url": {"@id": "schema:url"},
        "children": {"@id": "schema:children", "@type": "@id"}
      }
    }
  },
  "@type": "Person",
  "name": "%s",
  "url": "http://www.%s.com",
  "children": [{
    "@type": "Person",
    "name": "%s",
	"url": "http://www.%s.org"
  }]
}
`

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
	c := s.JSONLDCollection("json")
	var compoundIndex = c.NewIndex("compound",
		leia.NewFieldIndexer(leia.NewIRIPath("http://example.com/name"), leia.TransformerOption(leia.ToLower)),
		leia.NewFieldIndexer(leia.NewIRIPath("http://example.com/url")),
		leia.NewFieldIndexer(leia.NewIRIPath("http://example.com/children", "http://example.com/name")),
	)
	err = c.AddIndex(compoundIndex)
	if err != nil {
		panic(err)
	}

	// populate
	size := 8
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

	query := leia.New(leia.Eq(leia.NewIRIPath("http://example.com/name"), leia.MustParseScalar("name3"))).
		And(leia.Eq(leia.NewIRIPath("http://example.com/url"), leia.MustParseScalar("http://www.url3.com"))).
		And(leia.Eq(leia.NewIRIPath("http://example.com/children", "http://example.com/name"), leia.MustParseScalar("child3"))).
		And(leia.Eq(leia.NewIRIPath("http://example.com/children", "http://example.com/url"), leia.MustParseScalar("http://www.url3.org")))

	j, err := c.Find(context.Background(), query)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs\n", len(j))
}

func genJson(i, j, k, l int) leia.Document {
	v1 := fmt.Sprintf("Name%d", i)
	v2 := fmt.Sprintf("url%d", j)
	v3 := fmt.Sprintf("child%d", k)
	v4 := fmt.Sprintf("url%d", l)

	gen := fmt.Sprintf(personTemplate, v1, v2, v3, v4)

	return []byte(gen)
}
