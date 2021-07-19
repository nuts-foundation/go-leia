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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/nuts-foundation/go-leia"
)

func main() {
	// profiling
	//f, err := os.Create("profile")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()

	var credentialIndex = leia.NewIndex("subject.resource",
		leia.NewFieldIndexer("credentialSubject.id", leia.AliasOption{Alias: "subject"}),
		leia.NewFieldIndexer("credentialSubject.resources.#.path", leia.AliasOption{Alias: "resource"}, leia.TransformerOption{Transformer: leia.ToLower}),
	)

	dir, err := ioutil.TempDir("", "vcs")
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
	c := s.Collection("vcs")
	if err != nil {
		panic(err)
	}
	err = c.AddIndex(credentialIndex)
	if err != nil {
		panic(err)
	}

	// populate
	issuers := 10
	subjects := 50 // 100
	total := 1000 // 10000

	genJson(issuers, subjects, total, c)
	fmt.Printf("added %d docs\n", total*subjects*issuers)

	query := leia.New(leia.Eq("subject", "did:nuts:subject_8")).
		And(leia.Eq("resource", "/resource/15/8_9"))

	t := time.Now()
	j, err := c.Find(query)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found %d docs in %s\n", len(j), time.Now().Sub(t).String())
}

type credential struct {
	Issuer            string            `json:"issuer"`
	IssuanceDate      string            `json:"issuanceDate"`
	CredentialSubject credentialSubject `json:"credentialSubject"`
}
type credentialSubject struct {
	PurposeOfUse string     `json:"purposeOfUse"`
	ID           string     `json:"id"`
	Resources    []resource `json:"resources"`
}
type resource struct {
	Path        string   `json:"path"`
	Operations  []string `json:"operations"`
	UserContext bool     `json:"userContext"`
}

func genJson(issuers, subjects, total int, collection leia.Collection) {
	startDate := time.Time{}.AddDate(2010, 1, 1)

	for i := 0; i < issuers; i++ {
		for s := 0; s < subjects; s++ {
			docs := make([]leia.Document, 0)
			for t := 0; t < total; t++ {
				result := credential{
					Issuer:       fmt.Sprintf("did:nuts:issuer_%d", i),
					IssuanceDate: startDate.Format(time.RFC3339),
					CredentialSubject: credentialSubject{
						PurposeOfUse: "service",
						ID:           fmt.Sprintf("did:nuts:subject_%d", s),
						Resources:    make([]resource, 20),
					},
				}
				for r := 0; r < 20; r++ {
					result.CredentialSubject.Resources[r] = resource{
						Path:        fmt.Sprintf("/resource/%d/%d_%d", r, i, t),
						Operations:  []string{"read"},
						UserContext: true,
					}
				}

				bytes, err := json.Marshal(result)
				if err != nil {
					panic(err)
				}
				docs = append(docs, leia.DocumentFromBytes(bytes))

				startDate = startDate.AddDate(0, 0, 1)
			}
			err := collection.Add(docs)
			if err != nil {
				panic(err)
			}
		}
	}
}
