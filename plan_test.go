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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

func TestFullTableScanQueryPlan_execute(t *testing.T) {
	t.Run("ok - returns nil when no globalDocument bucket exists", func(t *testing.T) {
		_, c := testCollection(t)
		queryPlan := fullTableScanQueryPlan{
			queryPlanBase: queryPlanBase{
				collection: c,
			},
		}

		err := queryPlan.execute(func(key Reference, value []byte) error {
			// should not be called
			return errors.New("failed")
		})

		assert.NoError(t, err)
	})

	t.Run("error - when walker returns an error", func(t *testing.T) {
		_, c := testCollection(t)
		_ = c.Add([]Document{exampleDoc})
		queryPlan := fullTableScanQueryPlan{
			queryPlanBase: queryPlanBase{
				collection: c,
			},
		}

		err := queryPlan.execute(func(key Reference, value []byte) error {
			// should not be called
			return errors.New("failed")
		})

		assert.EqualError(t, err, "failed")
	})
}

func TestIndexScanQueryPlan_Execute(t *testing.T) {
	t.Run("error - query does not exactly match index", func(t *testing.T) {
		_, _, i := testIndex(t)
		queryPlan := indexScanQueryPlan{
			queryPlanBase: queryPlanBase{
				query: New(Eq(NewJSONPath("key"), valueAsScalar)).And(Eq(NewJSONPath("not_indexed"), valueAsScalar)),
			},
			index: i,
		}

		err := queryPlan.execute(func(key []byte, value []byte) error {
			// should not be called
			return errors.New("failed in loop")
		})

		assert.EqualError(t, err, "no index with exact match to query found")
	})

	t.Run("ok - nothing added", func(t *testing.T) {
		_, c, i := testIndex(t)
		queryPlan := indexScanQueryPlan{
			queryPlanBase: queryPlanBase{
				collection: c,
				query:      New(Eq(NewJSONPath("path.part"), valueAsScalar)),
			},
			index: i,
		}

		err := queryPlan.execute(func(key []byte, value []byte) error {
			// should not be called
			return errors.New("failed")
		})

		assert.NoError(t, err)
	})
}

func TestResultScanQueryPlan_Execute(t *testing.T) {
	t.Run("ok - nothing added", func(t *testing.T) {
		_, c, i := testIndex(t)
		queryPlan := resultScanQueryPlan{
			queryPlanBase: queryPlanBase{
				collection: c,
				query:      New(Eq(NewJSONPath("key"), valueAsScalar)),
			},
			index: i,
		}

		err := queryPlan.execute(func(key Reference, value []byte) error {
			// should not be called
			return errors.New("failed")
		})

		assert.NoError(t, err)
	})
}

func TestDocumentFetcher(t *testing.T) {
	t.Run("ok - nil bytes passed", func(t *testing.T) {
		db, c := testCollection(t)
		_ = c.Add([]Document{exampleDoc})

		err := db.View(func(tx *bbolt.Tx) error {
			fetcher := documentFetcher(tx.Bucket(documentCollectionByteRef()), func(_ []byte, _ []byte) error {
				return errors.New("failed")
			})

			return fetcher(nil, nil)
		})

		assert.NoError(t, err)
	})
}

func TestResultScanner(t *testing.T) {
	json := `
{
	"id": 1,
	"main": {
		"nesting": {
			"type": "bird",
			"nice": false
		}
	}
}
`

	bytes := []byte(json)

	t.Run("error - non comparable entry", func(t *testing.T) {
		db, c := testCollection(t)
		_ = c.Add([]Document{exampleDoc})

		err := db.View(func(tx *bbolt.Tx) error {
			scanner := resultScanner([]QueryPart{Eq(NewJSONPath("main.nesting"), valueAsScalar)}, func(_ Reference, _ []byte) error {
				return errors.New("failed")
			}, c)

			return scanner(nil, bytes)
		})

		assert.EqualError(t, err, "type at path not supported for indexing: {\n\t\t\t\"type\": \"bird\",\n\t\t\t\"nice\": false\n\t\t}")
	})
}
