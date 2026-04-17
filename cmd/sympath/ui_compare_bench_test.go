package main

import (
	"context"
	"database/sql"
	"os"
	"testing"
)

// defaultCompareBenchmarkDB points at the copied local analysis database used
// during development. Other machines will usually need SYMPATH_BENCH_DB to
// point at their own copied inventory database.
const defaultCompareBenchmarkDB = "/tmp/4H5j2SviZF.sympath.analysis-copy"

var benchmarkCompareResultSink compareResult

type compareByPathBenchmarkCase struct {
	name        string
	leftScan    int64
	rightScan   int64
	leftPrefix  string
	rightPrefix string
}

type compareSQLFirstSetup struct {
	cte       string
	args      []any
	leftOnly  []fileEntry
	rightOnly []fileEntry
}

func BenchmarkCompareByPath(b *testing.B) {
	dbPath := os.Getenv("SYMPATH_BENCH_DB")
	if dbPath == "" {
		dbPath = defaultCompareBenchmarkDB
	}
	if _, err := os.Stat(dbPath); err != nil {
		b.Skipf("benchmark database unavailable at %q: %v", dbPath, err)
	}

	db, err := openUIReadOnlyDB(context.Background(), dbPath)
	if err != nil {
		b.Fatalf("open benchmark database: %v", err)
	}
	b.Cleanup(func() {
		_ = db.Close()
	})

	cases := []compareByPathBenchmarkCase{
		{
			name:      "whole_tree_exact_heavy_media_mirror",
			leftScan:  10,
			rightScan: 15,
		},
		{
			name:        "prefix_exact_heavy_music_mirror",
			leftScan:    11,
			rightScan:   15,
			rightPrefix: "music",
		},
		{
			name:      "whole_tree_mostly_unmatched_music_vs_library",
			leftScan:  6,
			rightScan: 11,
		},
		{
			name:        "normalization_heavy_album_prefix",
			leftScan:    10,
			rightScan:   15,
			leftPrefix:  "music/flac/Bigflo & Oli/La Vie de re\u0302ve [2018]",
			rightPrefix: "music/flac/Bigflo & Oli/La Vie de r\u00eave [2018]",
		},
	}

	benchmarks := []struct {
		name string
		fn   func(context.Context, *sql.DB, int64, int64, string, string, bool) (compareResult, error)
	}{
		{name: "in_memory_full_materialize", fn: compareByPathInMemory},
		{name: "current_hybrid_sql_first", fn: compareByPathSQLFirst},
		{name: "adaptive_production", fn: compareByPath},
	}

	for _, bc := range cases {
		bc := bc
		b.Run(bc.name, func(b *testing.B) {
			for _, bench := range benchmarks {
				bench := bench
				b.Run(bench.name, func(b *testing.B) {
					b.ReportAllocs()
					ctx := context.Background()
					for i := 0; i < b.N; i++ {
						result, err := bench.fn(ctx, db, bc.leftScan, bc.rightScan, bc.leftPrefix, bc.rightPrefix, false)
						if err != nil {
							b.Fatalf("benchmark compare failed: %v", err)
						}
						benchmarkCompareResultSink = result
					}
				})
			}
		})
	}
}

func BenchmarkCompareByPathSQLFirstPhases(b *testing.B) {
	dbPath := os.Getenv("SYMPATH_BENCH_DB")
	if dbPath == "" {
		dbPath = defaultCompareBenchmarkDB
	}
	if _, err := os.Stat(dbPath); err != nil {
		b.Skipf("benchmark database unavailable at %q: %v", dbPath, err)
	}

	db, err := openUIReadOnlyDB(context.Background(), dbPath)
	if err != nil {
		b.Fatalf("open benchmark database: %v", err)
	}
	b.Cleanup(func() {
		_ = db.Close()
	})

	setup, err := prepareCompareSQLFirstSetup(context.Background(), db, 10, 15, "", "", false)
	if err != nil {
		b.Fatalf("prepare sql-first benchmark setup: %v", err)
	}

	b.Run("phase_one", func(b *testing.B) {
		b.ReportAllocs()
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			result, leftEntries, rightEntries, err := queryCompareByPathPhaseOneCTE(ctx, db, setup.cte, setup.args)
			if err != nil {
				b.Fatalf("phase one failed: %v", err)
			}
			benchmarkCompareResultSink = result
			if len(leftEntries)+len(rightEntries) == 0 {
				b.Fatal("unexpected empty unmatched sets")
			}
		}
	})

	b.Run("compact_left_only", func(b *testing.B) {
		b.ReportAllocs()
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			compact, err := compactMissingTreesFromComparePathsCTE(ctx, db, setup.cte, setup.args, setup.leftOnly, "right_entries")
			if err != nil {
				b.Fatalf("compact left-only failed: %v", err)
			}
			if len(compact) == 0 {
				b.Fatal("unexpected empty left compact result")
			}
		}
	})

	b.Run("compact_right_only", func(b *testing.B) {
		b.ReportAllocs()
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			compact, err := compactMissingTreesFromComparePathsCTE(ctx, db, setup.cte, setup.args, setup.rightOnly, "left_entries")
			if err != nil {
				b.Fatalf("compact right-only failed: %v", err)
			}
			if len(compact) == 0 {
				b.Fatal("unexpected empty right compact result")
			}
		}
	})
}

func prepareCompareSQLFirstSetup(ctx context.Context, db *sql.DB, leftScan, rightScan int64, leftPrefix, rightPrefix string, ignoreCommonOS bool) (compareSQLFirstSetup, error) {
	cte, args := entryCTEs(leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)
	result, leftEntries, rightEntries, err := queryCompareByPathPhaseOneCTE(ctx, db, cte, args)
	if err != nil {
		return compareSQLFirstSetup{}, err
	}
	leftOnly, rightOnly := reconcileComparePathFallback(&result, leftEntries, rightEntries)
	return compareSQLFirstSetup{
		cte:       cte,
		args:      args,
		leftOnly:  leftOnly,
		rightOnly: rightOnly,
	}, nil
}
