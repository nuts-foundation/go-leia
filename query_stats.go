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
//   - DocumentsScannedBytes vs DocumentsMatchedBytes: Shows data volume overhead.
//     Large difference indicates wasted I/O and unmarshalling work.
//
//   - Low DocumentsMatched/DocumentsScanned ratio: Suggests many documents were unnecessarily scanned.
//     A ratio < 0.1 (10%) means 90% of the work was wasted and a better index would provide significant benefit.
//
//   - SuggestedFields: Fields that should be indexed together (compound index) to improve performance.
//     Compare with the index definition of IndexUsed to see which fields are missing.
//
//   - IndexUsed: If empty string, no index was used (full table scan). If set, this index was used
//     but didn't fully cover the query conditions (check SuggestedFields to see what's missing).
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
// Examples:
//
//  1. Full table scan: IndexUsed="", DocumentsScanned=10000, DocumentsMatched=5
//     This means 9,995 documents were scanned unnecessarily.
//     Action: Create index on SuggestedFields to reduce scans to just 5 documents.
//
//  2. Suboptimal index: Query for {name, city} with only name indexed:
//     IndexUsed="name_index", SuggestedFields=[name, city]
//     DocumentsScanned=100 (all matching name), DocumentsMatched=5 (matching name AND city)
//     FilterEfficiency=0.05 (5%) - very poor
//     Action: Create compound index on SuggestedFields to scan only 5 documents.
type IndexStats struct {
	// Collection is the name of the collection being queried
	Collection string
	// Query is the query that was executed (use Query.String() for readable format)
	Query Query
	// DocumentsScanned is the total number of documents examined.
	// With ideal indexes, this is equal or close to DocumentsMatched.
	DocumentsScanned int
	// DocumentsScannedBytes is the total size in bytes of all scanned documents (before filtering).
	// High values indicate significant I/O and unmarshalling overhead.
	DocumentsScannedBytes int
	// DocumentsMatched is the number of documents that satisfied all query conditions
	DocumentsMatched int
	// DocumentsMatchedBytes is the total size in bytes of all matched documents (after filtering).
	// This is the actual result set size.
	DocumentsMatchedBytes int
	// SuggestedFields lists the query fields that should be indexed together to eliminate table scans or result set scans.
	// For suboptimal indexes, compare with the used index definition to see which fields are missing.
	SuggestedFields []string

	// IndexUsed is the name of the index that was used for this query.
	// Empty string means no index was used (full table scan).
	IndexUsed string
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
	//
	// Concurrency: This callback may be invoked concurrently from multiple goroutines when
	// queries run in parallel (for example when using bbolt's concurrent readers). The Leia
	// library does not serialize calls to this function. Implementations of OnIndexProblem
	// MUST therefore be goroutine-safe (e.g. by using synchronization when accessing shared
	// state, or by only calling goroutine-safe functions such as most loggers).
	OnIndexProblem func(stats IndexStats)

	// SuboptimalIndexThreshold is the number of wasted document scans that must be exceeded to trigger OnIndexProblem for indexed queries.
	// Wasted scans = DocumentsScanned - DocumentsMatched (documents that were fetched but filtered out).
	// The callback triggers when wastedScans > SuboptimalIndexThreshold (strictly greater than).
	// Full table scans with query conditions are always reported (regardless of threshold).
	// Default is 3 if not set (triggers when more than 3 documents are wasted).
	// Increase for noisier systems, decrease for stricter monitoring.
	SuboptimalIndexThreshold int
}

func suggestIndexFields(query Query) []string {
	fields := make([]string, len(query.parts))
	for i, part := range query.parts {
		fields[i] = queryPathString(part.QueryPath())
	}
	return fields
}

var NoOpQueryStatsCallbacks = QueryStatsCallbacks{}
