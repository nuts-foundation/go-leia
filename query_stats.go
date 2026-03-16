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

// IndexStats contains information about query performance and index usage.
// This is reported for both full table scans (no index) and suboptimal index usage (partial coverage).
//
// How to interpret:
//
//   - High DocumentsScanned: Indicates many documents were examined.
//     For full table scans (IndexUsed == ""), this is the entire collection.
//     With ideal indexes, DocumentsScanned should equal or be close to DocumentsMatched.
//
//   - Low DocumentsMatched/DocumentsScanned ratio: Suggests many documents were unnecessarily scanned.
//     A ratio < 0.1 (10%) means 90% of the work was wasted and a better index would provide significant benefit.
//
//   - Large ResultSetBytes: Even if matched docs are few, large result sizes can impact memory and network.
//
//   - SuggestedFields: Fields that should be indexed together (compound index) to improve performance.
//
//   - IndexUsed: If empty string, no index was used (full table scan). If set, this index was used
//     but didn't fully cover the query conditions.
//
//   - FilterEfficiency: For full table scans (IndexUsed == ""), this is 0.0.
//     For indexed queries, this is the ratio of DocumentsMatched/DocumentsScanned (0.0 to 1.0)
//
//   - 1.0 (100%): Perfect - every document from the index matched (index fully covers query)
//
//   - 0.5 (50%): Half the fetched documents were discarded after filtering
//
//   - 0.1 (10%): Only 10% of fetched docs matched - 90% were wasted (very poor efficiency)
//
//   - < 0.1: Critical - strong indicator that a compound index is needed
//
//   - QueryPartsInIndex/QueryPartsOutsideIndex:
//     For full table scans: QueryPartsInIndex=0, QueryPartsOutsideIndex=total query parts
//     For indexed queries: Shows how many parts are covered by the index vs filtered in-memory
//     If 1 part is in index and 2 are outside, the index helps narrow results but significant
//     filtering happens in-memory. A compound index covering all parts would be more efficient.
//
// Examples:
//
//  1. Full table scan: IndexUsed="", QueryPartsInIndex=0, DocumentsScanned=10000, DocumentsMatched=5
//     This means 9,995 documents were scanned unnecessarily.
//     Action: Create index on SuggestedFields to reduce scans to just 5 documents.
//
//  2. Suboptimal index: Query for {name="Alice", city="NYC"} with only name indexed:
//     IndexUsed="name_index", QueryPartsInIndex=1, QueryPartsOutsideIndex=1
//     DocumentsScanned=100 (all Alices), DocumentsMatched=5 (Alices in NYC)
//     FilterEfficiency=0.05 (5%) - very poor
//     Action: Create compound index on (name, city) to scan only 5 documents.
type IndexStats struct {
	// Collection is the name of the collection being queried
	Collection string
	// Query is the query that was executed (use Query.String() for readable format)
	Query Query
	// DocumentsScanned is the total number of documents examined.
	// With ideal indexes, this is equal or close to DocumentsMatched.
	DocumentsScanned int
	// DocumentsMatched is the number of documents that satisfied all query conditions
	DocumentsMatched int
	// ResultSetBytes is the total size in bytes of all matched documents
	ResultSetBytes int
	// SuggestedFields lists the query fields that should be indexed together to eliminate table scans or result set scans
	SuggestedFields []string

	// IndexUsed is the name of the index that was used for this query.
	// Empty string means no index was used (full table scan).
	IndexUsed string
	// QueryPartsInIndex is the number of query conditions covered by the index.
	// Zero for full table scans.
	QueryPartsInIndex int
	// QueryPartsOutsideIndex is the number of query conditions that required in-memory filtering.
	// For full table scans, this equals the total number of query parts.
	QueryPartsOutsideIndex int
	// FilterEfficiency is the ratio of matched/scanned documents (0.0 to 1.0).
	// Zero for full table scans. Lower values indicate more wasted work.
	// Values < 0.1 suggest a compound index is needed.
	FilterEfficiency float64
}

// QueryStatsCallbacks defines callback functions for query performance monitoring.
// Configure these callbacks to identify missing or suboptimal indexes in your queries.
//
// Usage:
//
//	callbacks := leia.QueryStatsCallbacks{
//	    OnIndexProblem: func(stats leia.IndexStats) {
//	        if stats.IndexUsed == "" {
//	            // Full table scan
//	            log.Printf("Full table scan! Query: %s, Add index on: %v",
//	                stats.Query.String(), stats.SuggestedFields)
//	        } else if stats.FilterEfficiency < 0.1 {
//	            // Suboptimal index with low efficiency
//	            log.Printf("Inefficient query! Index=%s, Efficiency=%.0f%%, Consider compound index on: %v",
//	                stats.IndexUsed, stats.FilterEfficiency*100, stats.SuggestedFields)
//	        }
//	    },
//	    SuboptimalIndexThreshold: 3,
//	}
//	store, _ := leia.NewStore(dbPath, leia.WithQueryStatsCallbacks(callbacks))
//
// When to act:
//   - IndexUsed == "" (full table scan) always indicates a missing index if the query has conditions
//   - FilterEfficiency < 0.1 suggests high-priority optimization needed
//   - DocumentsScanned > 100 should be investigated regardless of efficiency
type QueryStatsCallbacks struct {
	// OnIndexProblem is called when a query has performance concerns.
	// Check IndexUsed == "" to determine if it's a full table scan or suboptimal index usage.
	// For full table scans, add an index on SuggestedFields.
	// For suboptimal indexes, consider creating a compound index that includes all query fields.
	OnIndexProblem func(stats IndexStats)

	// SuboptimalIndexThreshold is the minimum number of wasted document scans to trigger OnIndexProblem for indexed queries.
	// Wasted scans = DocumentsScanned - DocumentsMatched (documents that were fetched but filtered out).
	// Full table scans are always reported. Default is 3 if not set.
	// Increase for noisier systems, decrease for stricter monitoring.
	SuboptimalIndexThreshold int
}

func formatQueryPath(qp QueryPath) string {
	switch p := qp.(type) {
	case jsonPath:
		return string(p)
	case iriPath:
		if len(p.iris) == 0 {
			return "(root)"
		}
		result := ""
		for i, iri := range p.iris {
			if i > 0 {
				result += " -> "
			}
			result += iri
		}
		return result
	default:
		return "(unknown)"
	}
}

func suggestIndexFields(query Query) []string {
	fields := make([]string, len(query.parts))
	for i, part := range query.parts {
		fields[i] = formatQueryPath(part.QueryPath())
	}
	return fields
}

var NoOpQueryStatsCallbacks = QueryStatsCallbacks{}
