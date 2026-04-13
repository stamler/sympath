package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	inventory "sympath"

	_ "modernc.org/sqlite"
)

type verboseLogger struct {
	w       io.Writer
	enabled bool
}

func newVerboseLogger(w io.Writer, enabled bool) verboseLogger {
	return verboseLogger{w: w, enabled: enabled}
}

func (l verboseLogger) Printf(format string, args ...any) {
	if !l.enabled {
		return
	}
	fmt.Fprintf(l.w, format+"\n", args...)
}

func (l verboseLogger) Warnf(format string, args ...any) {
	if l.w == nil {
		return
	}
	fmt.Fprintf(l.w, "Warning: "+format+"\n", args...)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	return runWithIO(args, os.Stdout, os.Stderr)
}

func runWithIO(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return runScanWithIO(nil, stdout, stderr)
	}

	switch args[0] {
	case "-h", "--help", "help":
		printUsage(stdout)
		return nil
	case "scan":
		return runScanWithIO(args[1:], stdout, stderr)
	default:
		return runScanWithIO(args, stdout, stderr)
	}
}

func runScanWithIO(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	verbose := fs.Bool("verbose", false, "Print startup database details")

	fs.Usage = func() {
		printUsage(stderr)
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	root := "."
	if fs.NArg() > 1 {
		return errors.New("scan accepts at most one root path")
	}
	if fs.NArg() == 1 {
		root = fs.Arg(0)
	}

	logger := newVerboseLogger(stderr, *verbose)

	ctx := context.Background()
	startup, err := resolveRunDBPath(ctx, shellRemoteTransport{}, logger)
	if err != nil {
		return err
	}

	db, err := sql.Open("sqlite", startup.DBPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	if err := inventory.PrepareLocalMachineDB(ctx, db, startup.Identity); err != nil {
		return fmt.Errorf("prepare database: %w", err)
	}
	if err := inventory.InventoryTree(ctx, db, root); err != nil {
		return fmt.Errorf("inventory tree: %w", err)
	}

	normalizedRoot, err := normalizePath(root)
	if err != nil {
		return fmt.Errorf("normalize root path: %w", err)
	}

	normalizedDB, err := normalizeDBPath(startup.DBPath)
	if err != nil {
		return fmt.Errorf("normalize database path: %w", err)
	}
	logger.Printf("Using database: %s", normalizedDB)

	summary, err := loadSummary(ctx, db, startup.Identity.MachineID, normalizedRoot)
	if err != nil {
		return fmt.Errorf("load scan summary: %w", err)
	}

	printSummary(stdout, normalizedRoot, normalizedDB, summary)
	return nil
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, "Usage:\n")
	fmt.Fprintf(w, "  sympath scan [--verbose] [ROOT]\n")
	fmt.Fprintf(w, "  sympath [--verbose] [ROOT]\n\n")
	fmt.Fprintf(w, "Scans ROOT into the consolidated SQLite inventory database in ~/.sympath.\n")
	fmt.Fprintf(w, "If ROOT is omitted, the current directory is scanned.\n")
	fmt.Fprintf(w, "Use --verbose to print database resolution and consolidation details.\n")
	fmt.Fprintf(w, "On startup, ~/.sympath/remotes is seeded if missing, remotes may be fetched, and ~/.sympath/*.sympath files are consolidated into one file.\n")
}

type scanSummary struct {
	ScanID int64
	Total  int
	States map[string]int
}

func loadSummary(ctx context.Context, db *sql.DB, machineID, root string) (scanSummary, error) {
	var summary scanSummary
	summary.States = make(map[string]int)

	if err := db.QueryRowContext(
		ctx,
		"SELECT current_scan_id FROM roots WHERE machine_id = ? AND root = ?",
		machineID, root,
	).Scan(&summary.ScanID); err != nil {
		return summary, err
	}

	if err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM entries WHERE scan_id = ?",
		summary.ScanID,
	).Scan(&summary.Total); err != nil {
		return summary, err
	}

	rows, err := db.QueryContext(
		ctx,
		"SELECT state, COUNT(*) FROM entries WHERE scan_id = ? GROUP BY state",
		summary.ScanID,
	)
	if err != nil {
		return summary, err
	}
	defer rows.Close()

	for rows.Next() {
		var state sql.NullString
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return summary, err
		}
		key := "unknown"
		if state.Valid && state.String != "" {
			key = state.String
		}
		summary.States[key] = count
	}

	return summary, rows.Err()
}

func printSummary(w io.Writer, root, dbPath string, summary scanSummary) {
	fmt.Fprintf(w, "Scan complete\n")
	fmt.Fprintf(w, "Root: %s\n", root)
	fmt.Fprintf(w, "DB:   %s\n", dbPath)
	fmt.Fprintf(w, "Scan: %d\n", summary.ScanID)
	fmt.Fprintf(w, "Files: %d", summary.Total)

	if len(summary.States) == 0 {
		fmt.Fprintln(w)
		return
	}

	parts := make([]string, 0, len(summary.States))
	for state, count := range summary.States {
		parts = append(parts, fmt.Sprintf("%s=%d", state, count))
	}
	sort.Strings(parts)
	fmt.Fprintf(w, " (%s)\n", strings.Join(parts, ", "))
}

func normalizePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return abs, nil
	}
	return resolved, nil
}

func normalizeDBPath(path string) (string, error) {
	if path == ":memory:" {
		return path, nil
	}
	return normalizePath(path)
}
