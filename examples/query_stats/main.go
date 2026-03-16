package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"

	leia "github.com/nuts-foundation/go-leia/v4"
)

func main() {
	tmpDir := os.TempDir()
	dbPath := path.Join(tmpDir, "example.db")
	defer os.Remove(dbPath)

	// Configure callbacks to track missing/suboptimal indexes
	callbacks := leia.QueryStatsCallbacks{
		OnIndexProblem: func(stats leia.IndexStats) {
			if stats.IndexUsed == "" {
				// Full table scan
				log.Printf("[WARN] Full table scan detected!\n")
				log.Printf("  Collection: %s\n", stats.Collection)
				log.Printf("  Query: %s\n", stats.Query.String())
				log.Printf("  Documents scanned: %d\n", stats.DocumentsScanned)
				log.Printf("  Documents matched: %d\n", stats.DocumentsMatched)
				log.Printf("  Result set size: %d bytes (%.2f KB)\n", stats.ResultSetBytes, float64(stats.ResultSetBytes)/1024)
				log.Printf("  Suggested index fields: %v\n", stats.SuggestedFields)
				log.Printf("  Consider adding an index on these fields to improve performance.\n\n")
			} else if stats.FilterEfficiency < 0.1 && stats.DocumentsScanned > 10 {
				// Suboptimal index with very low efficiency
				log.Printf("[WARN] Suboptimal index with very low efficiency!\n")
				log.Printf("  Collection: %s\n", stats.Collection)
				log.Printf("  Query: %s\n", stats.Query.String())
				log.Printf("  Index used: %s\n", stats.IndexUsed)
				log.Printf("  Query parts in index: %d\n", stats.QueryPartsInIndex)
				log.Printf("  Query parts outside index: %d\n", stats.QueryPartsOutsideIndex)
				log.Printf("  Documents scanned: %d\n", stats.DocumentsScanned)
				log.Printf("  Documents matched: %d\n", stats.DocumentsMatched)
				log.Printf("  Result set size: %d bytes (%.2f KB)\n", stats.ResultSetBytes, float64(stats.ResultSetBytes)/1024)
				log.Printf("  Filter efficiency: %.2f%%\n", stats.FilterEfficiency*100)
				log.Printf("  Suggested compound index fields: %v\n", stats.SuggestedFields)
				log.Printf("  Consider adding a compound index for better performance.\n\n")
			} else if stats.IndexUsed != "" {
				// Suboptimal index with moderate efficiency
				log.Printf("[INFO] Suboptimal index usage detected!\n")
				log.Printf("  Collection: %s\n", stats.Collection)
				log.Printf("  Query: %s\n", stats.Query.String())
				log.Printf("  Index used: %s\n", stats.IndexUsed)
				log.Printf("  Documents scanned: %d, matched: %d (%.1f%% efficiency)\n",
					stats.DocumentsScanned, stats.DocumentsMatched, stats.FilterEfficiency*100)
				log.Printf("  Consider compound index on: %v\n\n", stats.SuggestedFields)
			}
		},
		SuboptimalIndexThreshold: 3,
	}

	// Create store with query stats callbacks
	store, err := leia.NewStore(dbPath, leia.WithQueryStatsCallbacks(callbacks))
	if err != nil {
		panic(err)
	}
	defer store.Close()

	// Create collection with only a partial index
	collection := store.Collection(leia.JSONCollection, "users")
	nameIndex := collection.NewIndex("name_index",
		leia.NewFieldIndexer(leia.NewJSONPath("name")))
	if err := collection.AddIndex(nameIndex); err != nil {
		panic(err)
	}

	// Add some test data
	docs := []leia.Document{
		[]byte(`{"name": "Alice", "age": 30, "city": "New York"}`),
		[]byte(`{"name": "Alice", "age": 25, "city": "Los Angeles"}`),
		[]byte(`{"name": "Alice", "age": 35, "city": "San Francisco"}`),
		[]byte(`{"name": "Alice", "age": 40, "city": "Boston"}`),
		[]byte(`{"name": "Alice", "age": 45, "city": "Seattle"}`),
		[]byte(`{"name": "Bob", "age": 30, "city": "New York"}`),
		[]byte(`{"name": "Charlie", "age": 35, "city": "Chicago"}`),
	}

	if err := collection.Add(docs); err != nil {
		panic(err)
	}

	fmt.Println("=== Example 1: Full Table Scan (Missing Index) ===")
	// Query without matching index - triggers OnFullTableScan
	query1 := leia.New(leia.Eq(leia.NewJSONPath("city"), leia.MustParseScalar("New York")))
	results1, err := collection.Find(context.Background(), query1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found %d users in New York\n\n", len(results1))

	fmt.Println("=== Example 2: Suboptimal Index (Needs Compound Index) ===")
	// Query uses name index but has to filter many results - triggers OnSuboptimalIndex
	query2 := leia.New(leia.Eq(leia.NewJSONPath("name"), leia.MustParseScalar("Alice"))).
		And(leia.Eq(leia.NewJSONPath("city"), leia.MustParseScalar("Boston")))
	results2, err := collection.Find(context.Background(), query2)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found %d users named Alice in Boston\n\n", len(results2))

	fmt.Println("=== Example 3: Efficient Query (Below Threshold) ===")
	// Query uses index efficiently - no callback triggered
	query3 := leia.New(leia.Eq(leia.NewJSONPath("name"), leia.MustParseScalar("Bob")))
	results3, err := collection.Find(context.Background(), query3)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found %d users named Bob (no warning - efficient query)\n", len(results3))
}
