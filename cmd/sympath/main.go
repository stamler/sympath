package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	inventory "sympath"

	_ "modernc.org/sqlite"
)

var version = "dev"

var newScanContext = func() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

var inventoryScanWithProgress = inventory.InventoryTreeWithProgress

const staleScanAdoptionTimeout = 5 * time.Second

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
	var updates *autoUpdateCheck
	if shouldAutoCheck(args) {
		updates = newUpdateChecker().startAutoCheck(version)
	}

	if len(args) == 0 {
		if err := runScanWithIO(nil, stdout, stderr); err != nil {
			return err
		}
		return emitAutoUpdateNotice(stderr, updates)
	}

	var err error
	switch args[0] {
	case "-h", "--help", "help":
		printUsage(stdout)
		return nil
	case "-version", "--version", "version":
		fmt.Fprintln(stdout, version)
	case "update":
		err = runUpdateWithIO(args[1:], stdout, stderr)
	case "update-check":
		err = runUpdateCheckWithIO(args[1:], stdout, stderr)
	case "scan":
		err = runScanWithIO(args[1:], stdout, stderr)
	case "ui":
		err = runUIWithIO(args[1:], stdout, stderr)
	default:
		err = runScanWithIO(args, stdout, stderr)
	}

	if err != nil {
		return err
	}
	return emitAutoUpdateNotice(stderr, updates)
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

	logger := newScanLogger(stderr, *verbose)

	ctx, stop := newScanContext()
	defer stop()
	stateDir, err := sympathStateDir()
	if err != nil {
		return fmt.Errorf("resolve sympath state directory: %w", err)
	}
	if err := ensureSympathDir(stateDir, logger); err != nil {
		return err
	}
	normalizedRoot, err := normalizePath(root)
	if err != nil {
		return fmt.Errorf("normalize root path: %w", err)
	}
	logger.Debugf("Preparing scan for %s", normalizedRoot)
	startupLock, err := acquireScanStartupLock(ctx, stateDir)
	if err != nil {
		return fmt.Errorf("acquire scan startup lock: %w", err)
	}
	startupLockReleased := false
	defer func() {
		if !startupLockReleased {
			_ = startupLock.Close()
		}
	}()

	exclusiveDBGuard, exclusiveDBGuardOK, err := tryAcquireScanDBGuardLockExclusive(stateDir)
	if err != nil {
		return fmt.Errorf("acquire scan database guard: %w", err)
	}

	var startup startupState
	var dbGuard *fileLock
	var rootLock *scanLock
	var db *sql.DB
	if exclusiveDBGuardOK {
		startup, err = resolveRunDBPath(ctx, shellRemoteTransport{}, logger)
		if err != nil {
			_ = exclusiveDBGuard.Close()
			return err
		}
		rootLock, err = acquireScanRootLock(stateDir, startup.Identity.MachineID, normalizedRoot)
		if err != nil {
			_ = exclusiveDBGuard.Close()
			return err
		}
		db, err = sql.Open("sqlite", startup.DBPath)
		if err != nil {
			_ = rootLock.Close()
			_ = exclusiveDBGuard.Close()
			return fmt.Errorf("open database: %w", err)
		}
		if err := inventory.PrepareLocalMachineDB(ctx, db, startup.Identity); err != nil {
			_ = db.Close()
			_ = rootLock.Close()
			_ = exclusiveDBGuard.Close()
			return fmt.Errorf("prepare database: %w", err)
		}
		if err := inventory.EnsureRelPathNormBackfill(ctx, db); err != nil {
			_ = db.Close()
			_ = rootLock.Close()
			_ = exclusiveDBGuard.Close()
			return fmt.Errorf("prepare normalization data: %w", err)
		}
		if err := exclusiveDBGuard.Close(); err != nil {
			_ = db.Close()
			_ = rootLock.Close()
			return fmt.Errorf("release exclusive database guard: %w", err)
		}
		dbGuard, err = acquireScanDBGuardLockShared(ctx, stateDir)
		if err != nil {
			_ = db.Close()
			_ = rootLock.Close()
			return fmt.Errorf("acquire shared database guard: %w", err)
		}
	} else {
		dbGuard, err = acquireScanDBGuardLockShared(ctx, stateDir)
		if err != nil {
			return fmt.Errorf("acquire shared database guard: %w", err)
		}
		startup, err = resolveExistingRunDBPath(logger)
		if err != nil {
			_ = dbGuard.Close()
			return err
		}
		rootLock, err = acquireScanRootLock(stateDir, startup.Identity.MachineID, normalizedRoot)
		if err != nil {
			_ = dbGuard.Close()
			return err
		}
		db, err = sql.Open("sqlite", startup.DBPath)
		if err != nil {
			_ = rootLock.Close()
			_ = dbGuard.Close()
			return fmt.Errorf("open database: %w", err)
		}
		if err := inventory.PrepareLocalMachineDB(ctx, db, startup.Identity); err != nil {
			_ = db.Close()
			_ = rootLock.Close()
			_ = dbGuard.Close()
			return fmt.Errorf("prepare database: %w", err)
		}
		if err := inventory.EnsureRelPathNormBackfill(ctx, db); err != nil {
			_ = db.Close()
			_ = rootLock.Close()
			_ = dbGuard.Close()
			return fmt.Errorf("prepare normalization data: %w", err)
		}
	}
	defer dbGuard.Close()
	defer rootLock.Close()
	defer db.Close()

	if err := startupLock.Close(); err != nil {
		return fmt.Errorf("release scan startup lock: %w", err)
	}
	startupLockReleased = true

	adoptCtx, cancelAdopt := context.WithTimeout(context.Background(), staleScanAdoptionTimeout)
	if err := markRunningScansFailed(adoptCtx, db, startup.Identity.MachineID, normalizedRoot); err != nil {
		cancelAdopt()
		return fmt.Errorf("mark stale running scans failed: %w", err)
	}
	cancelAdopt()

	if logger.Enabled(logLevelDebug) {
		interruptedScanID, reusableCount, err := loadInterruptedResumeInfo(ctx, db, startup.Identity.MachineID, normalizedRoot)
		if err != nil {
			return fmt.Errorf("load interrupted scan resume info: %w", err)
		}
		if interruptedScanID != 0 {
			logger.Debugf("Interrupted resume source: scan %d (%d reusable entries)", interruptedScanID, reusableCount)
		}
	}

	logger.Debugf("Starting local scan of %s", normalizedRoot)
	progress, display := newScanProgressDisplay(logger)
	display.Start()

	if err := inventoryScanWithProgress(ctx, db, root, progress); err != nil {
		if display != nil {
			display.Stop()
		}
		return fmt.Errorf("inventory tree: %w", err)
	}
	if display != nil {
		display.Stop()
	}

	normalizedDB, err := normalizeDBPath(startup.DBPath)
	if err != nil {
		return fmt.Errorf("normalize database path: %w", err)
	}
	logger.Debugf("Using database: %s", normalizedDB)

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
	fmt.Fprintf(w, "  sympath ui\n")
	fmt.Fprintf(w, "  sympath version\n")
	fmt.Fprintf(w, "  sympath update\n")
	fmt.Fprintf(w, "  sympath update-check\n")
	fmt.Fprintf(w, "  sympath [--verbose] [ROOT]\n\n")
	fmt.Fprintf(w, "Scans ROOT into the consolidated SQLite inventory database in ~/.sympath.\n")
	fmt.Fprintf(w, "If ROOT is omitted, the current directory is scanned.\n")
	fmt.Fprintf(w, "Use --verbose to add debug-level startup and consolidation details.\n")
	fmt.Fprintf(w, "On startup, ~/.sympath/remotes is seeded if missing, remotes may be fetched, and ~/.sympath/*.sympath files are consolidated into one file.\n\n")
	fmt.Fprintf(w, "The ui command consolidates local databases (skipping remote fetch), opens the\n")
	fmt.Fprintf(w, "chosen database as a read-only best-effort snapshot, then launches a web\n")
	fmt.Fprintf(w, "interface for comparing inventoried directory trees while later scans may proceed.\n")
	fmt.Fprintf(w, "Use sympath version or sympath --version to print the build version.\n")
	fmt.Fprintf(w, "Use sympath update to install a newer managed release in place.\n")
	fmt.Fprintf(w, "Use sympath update-check to force a live release check.\n")
	fmt.Fprintf(w, "Successful scan, ui, and version commands may print a brief stderr-only update notice.\n")
}

func shouldAutoCheck(args []string) bool {
	if strings.TrimSpace(os.Getenv(internalNoUpdateEnv)) == "1" {
		return false
	}
	if len(args) == 0 {
		return true
	}

	switch args[0] {
	case "-h", "--help", "help", "update", "update-check":
		return false
	default:
		return true
	}
}

func emitAutoUpdateNotice(stderr io.Writer, updates *autoUpdateCheck) error {
	if updates == nil {
		return nil
	}
	if notice := updates.notice(); notice != "" {
		fmt.Fprintln(stderr, notice)
	}
	return nil
}

func runUpdateCheckWithIO(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 {
		return errors.New("update-check accepts no arguments")
	}

	ctx, cancel := context.WithTimeout(context.Background(), updateCheckTimeout)
	defer cancel()

	status, err := newUpdateChecker().resolveStatus(ctx, version, true)
	if err != nil {
		return fmt.Errorf("live check failed: %w", err)
	}

	fmt.Fprintln(stdout, formatUpdateCheckMessage(status))
	return nil
}

func runUpdateWithIO(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 {
		return errors.New("update accepts no arguments")
	}

	if err := validateTaggedBuildVersion(version); err != nil {
		return err
	}

	checker := newUpdateChecker().withDefaults()
	targetVersion := requestedInstallVersion()
	if targetVersion != "" {
		if _, err := parseTaggedVersion(targetVersion); err != nil {
			return fmt.Errorf("invalid %s %q: %w", installVersionEnv, targetVersion, err)
		}
		if targetVersion == version {
			fmt.Fprintf(stdout, "%s is up to date\n", version)
			return nil
		}
	} else {
		statusCtx, cancel := context.WithTimeout(context.Background(), updateCheckTimeout)
		defer cancel()

		status, err := checker.resolveStatus(statusCtx, version, true)
		if err != nil {
			return fmt.Errorf("live check failed: %w", err)
		}

		if !status.UpdateAvailable {
			fmt.Fprintln(stdout, formatUpdateCheckMessage(status))
			return nil
		}
		targetVersion = status.LatestVersion
	}

	if targetVersion == version {
		fmt.Fprintf(stdout, "%s is up to date\n", version)
		return nil
	}

	fmt.Fprintf(stdout, "Updating sympath from %s to %s...\n", version, targetVersion)

	installCtx, installCancel := context.WithTimeout(context.Background(), updateInstallTimeout)
	defer installCancel()

	result, err := installManagedRelease(installCtx, targetVersion)
	if err != nil {
		return err
	}

	if err := checker.writeCache(updateCache{
		CheckedAt:     checker.now().UTC(),
		LatestVersion: result.Version,
		ReleaseURL:    result.ReleaseURL,
	}); err != nil && stderr != nil {
		fmt.Fprintf(stderr, "Warning: failed to update release cache: %v\n", err)
	}

	if result.Scheduled {
		fmt.Fprintf(stdout, "Scheduled sympath %s for installation at %s\n", result.Version, result.TargetPath)
		fmt.Fprintln(stdout, "Restart sympath after this command exits.")
		return nil
	}

	fmt.Fprintf(stdout, "Installed sympath %s to %s\n", result.Version, result.TargetPath)
	return nil
}

func validateTaggedBuildVersion(currentVersion string) error {
	if _, err := parseTaggedVersion(currentVersion); err != nil {
		return fmt.Errorf("sympath update is unavailable for build %s; reinstall with: %s", currentVersion, reinstallCommandForGOOS(runtime.GOOS))
	}
	return nil
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

func loadInterruptedResumeInfo(ctx context.Context, db *sql.DB, machineID, root string) (int64, int, error) {
	var scanID sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT scan_id
		FROM scans
		WHERE machine_id = ? AND root = ? AND status = 'failed'
		ORDER BY scan_id DESC
		LIMIT 1
	`, machineID, root).Scan(&scanID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	if !scanID.Valid {
		return 0, 0, nil
	}

	var reusableCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM entries
		WHERE scan_id = ? AND state IN ('ok', 'reused')
	`, scanID.Int64).Scan(&reusableCount); err != nil {
		return 0, 0, err
	}

	return scanID.Int64, reusableCount, nil
}

func markRunningScansFailed(ctx context.Context, db *sql.DB, machineID, root string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE scans
		SET status = 'failed', finished_at = COALESCE(finished_at, ?)
		WHERE machine_id = ? AND root = ? AND status = 'running'
	`, time.Now().UnixNano(), machineID, root)
	return err
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
