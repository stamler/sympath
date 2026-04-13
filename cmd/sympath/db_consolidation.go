package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	inventory "sympath"

	"github.com/google/uuid"

	_ "modernc.org/sqlite"
)

// This file owns the CLI startup sequence around ~/.sympath.
//
// The inventory library knows how to scan one root into one database.
// The CLI adds multi-database startup behavior on top:
//
//   1. ensure ~/.sympath exists
//   2. ensure ~/.sympath/remotes exists, seeding it with a detailed
//      comment block if missing
//   3. ensure ~/.sympath/machine-id exists
//   4. optionally fetch remote .sympath files into temporary local files
//   5. consolidate local survivor data plus fetched remote data into one
//      working database
//   6. point the upcoming local scan at that working database
//
// The consolidation step must be idempotent. A designated aggregator may
// fetch the same remote machine on every startup, so repeated imports
// replace existing machine/root snapshots instead of stacking up
// duplicates over time. Removed remotes are handled via a small state
// file that remembers which machine IDs were previously imported from
// each configured alias.

const (
	remotesFileName      = "remotes"
	machineIDFileName    = "machine-id"
	remotesStateName     = "remotes.state.json"
	buildTempPrefix      = ".sympath-build-"
	fetchTempPrefix      = ".sympath-fetch-"
	remoteCommandTimeout = 30 * time.Second
	unknownHostnameName  = "unknown-host"
)

const remotesFileTemplate = `# sympath remotes configuration
#
# If this file contains one or more remote entries, this machine acts as
# the designated consolidator. On every startup, sympath reads this file,
# connects to each listed remote over SSH, copies that machine's current
# ~/.sympath/*.sympath database into the local ~/.sympath directory, and
# then consolidates the local and remote data before scanning the local
# root you asked for.
#
# HOW TO WRITE THIS FILE
# - One remote target per line.
# - Blank lines are ignored.
# - Lines beginning with # are ignored.
# - Each non-comment line must be a single SSH target token such as:
#     mac-mini
#     dean@fileserver
#     lab-host
#
# WHAT YOU MAY INCLUDE
# - An SSH config alias from ~/.ssh/config.
# - A plain host name.
# - A user@host target.
# - A root@host target, if that remote allows root SSH login.
#
# WHAT YOU MAY NOT INCLUDE
# - Multiple targets on one line.
# - Inline comments after a target.
# - Ports, paths, or extra SSH flags on the line.
# - Shell syntax, quoting, globs, commas, or separators.
# - A filesystem path such as ~/.sympath or /some/other/place.
#
# HOW TO HANDLE PORTS, KEYS, AND JUMP HOSTS
# Put those details in ~/.ssh/config. This file intentionally stays
# simple so the parser is predictable and the meaning of each line is
# unambiguous.
#
# REMOTE ACCOUNT BEHAVIOR
# - Each remote is fetched as the SSH login account in this file.
# - root@host reads /root/.sympath/*.sympath on that machine.
# - user@host reads that user's ~/.sympath/*.sympath instead.
#
# IMPORTANT OPERATIONAL NOTES
# - Only the machine with this file populated should aggregate remotes.
# - The remotes listed here should normally be leaf machines, not other
#   aggregators, or you may double-count already-consolidated data.
# - If a remote fetch fails, sympath warns and continues using the last
#   successfully consolidated view it already has for that machine.
# - If you remove a remote from this file, its previously imported machine
#   data is removed from the next successful consolidation run.
#
# Add remote SSH targets below this comment block, one per line.
`

type remoteTransport interface {
	LocateRemoteDB(ctx context.Context, target string) (string, error)
	FetchRemoteDB(ctx context.Context, target, remotePath, localPath string) error
}

type shellRemoteTransport struct{}

type startupState struct {
	DBPath   string
	Identity inventory.MachineIdentity
}

type remoteState map[string][]string

type fetchedRemote struct {
	Alias string
	Path  string
}

func resolveRunDBPath(ctx context.Context, transport remoteTransport, logger verboseLogger) (startupState, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return startupState{}, fmt.Errorf("resolve home directory for database path: %w", err)
	}
	dir := filepath.Join(home, ".sympath")

	if err := ensureSympathDir(dir, logger); err != nil {
		return startupState{}, err
	}

	identity, err := ensureMachineIdentity(dir, logger)
	if err != nil {
		return startupState{}, err
	}

	dbPath, err := consolidateSympathDir(ctx, dir, identity, transport, logger)
	if err != nil {
		return startupState{}, err
	}

	return startupState{
		DBPath:   dbPath,
		Identity: identity,
	}, nil
}

func consolidateSympathDir(
	ctx context.Context,
	dir string,
	identity inventory.MachineIdentity,
	transport remoteTransport,
	logger verboseLogger,
) (string, error) {
	if err := ensureSeededRemotesFile(dir, logger); err != nil {
		return "", err
	}
	remotes, err := loadRemoteTargets(filepath.Join(dir, remotesFileName))
	if err != nil {
		return "", err
	}
	statePath := filepath.Join(dir, remotesStateName)
	state, err := loadRemoteState(statePath)
	if err != nil {
		return "", err
	}

	fetched, fetchSucceeded, err := fetchRemotes(ctx, dir, remotes, transport, logger)
	if err != nil {
		cleanupFetchedFiles(fetched, logger)
		return "", err
	}

	localDBs, err := listSympathDBs(dir)
	if err != nil {
		cleanupFetchedFiles(fetched, logger)
		return "", err
	}

	removedMachineIDs := machineIDsForRemovedAliases(state, remotes)
	needsConsolidation := len(localDBs) > 1 || len(fetched) > 0 || len(removedMachineIDs) > 0

	if len(localDBs) == 0 && len(fetched) == 0 {
		filename, err := inventory.NewRandomSympathFilename()
		if err != nil {
			return "", fmt.Errorf("generate random database filename: %w", err)
		}
		dbPath := filepath.Join(dir, filename)
		logger.Printf("No .sympath databases found in %s", dir)
		logger.Printf("Created new database path: %s", dbPath)
		return dbPath, nil
	}

	if !needsConsolidation && len(localDBs) == 1 {
		logger.Printf("Using existing database without consolidation: %s", localDBs[0])
		return localDBs[0], nil
	}

	targetPath, err := newTempDBPath(dir, buildTempPrefix)
	if err != nil {
		cleanupFetchedFiles(fetched, logger)
		return "", err
	}

	targetDB, err := sql.Open("sqlite", targetPath)
	if err != nil {
		cleanupFetchedFiles(fetched, logger)
		return "", fmt.Errorf("open temporary consolidation database %q: %w", targetPath, err)
	}

	consolidationOK := false
	defer func() {
		targetDB.Close()
		if !consolidationOK {
			removeDBArtifacts(targetPath, logger)
			cleanupFetchedFiles(fetched, logger)
		}
	}()

	if err := inventory.PrepareLocalMachineDB(ctx, targetDB, identity); err != nil {
		return "", fmt.Errorf("prepare consolidation database %q: %w", targetPath, err)
	}

	logger.Printf("Consolidating %d local database file(s) and %d fetched remote file(s) in %s", len(localDBs), len(fetched), dir)
	for _, path := range localDBs {
		if err := importLocalDB(ctx, targetDB, path, identity, logger); err != nil {
			return "", err
		}
	}

	if len(removedMachineIDs) > 0 {
		logger.Printf("Removing data for machine IDs from deleted remotes: %s", strings.Join(removedMachineIDs, ", "))
		if err := inventory.DeleteMachineData(ctx, targetDB, removedMachineIDs); err != nil {
			return "", fmt.Errorf("purge deleted remote machine data: %w", err)
		}
	}

	nextState := cloneRemoteState(state)
	for _, remote := range fetched {
		summary, err := importFetchedRemote(ctx, targetDB, remote.Path, logger)
		if err != nil {
			return "", err
		}

		oldMachineIDs := nextState[remote.Alias]
		staleMachineIDs := difference(oldMachineIDs, summary.MachineIDs)
		if len(staleMachineIDs) > 0 {
			logger.Printf("Removing stale machine IDs for remote %s: %s", remote.Alias, strings.Join(staleMachineIDs, ", "))
			if err := inventory.DeleteMachineData(ctx, targetDB, staleMachineIDs); err != nil {
				return "", fmt.Errorf("purge stale remote data for %s: %w", remote.Alias, err)
			}
		}
		nextState[remote.Alias] = summary.MachineIDs
	}

	for alias := range nextState {
		if _, ok := fetchSucceeded[alias]; !ok && !contains(remotes, alias) {
			delete(nextState, alias)
		}
	}

	if err := targetDB.Close(); err != nil {
		return "", fmt.Errorf("close temporary consolidation database %q: %w", targetPath, err)
	}

	finalPath, err := newFinalDBPath(dir)
	if err != nil {
		cleanupFetchedFiles(fetched, logger)
		return "", err
	}
	if err := os.Rename(targetPath, finalPath); err != nil {
		cleanupFetchedFiles(fetched, logger)
		return "", fmt.Errorf("rename consolidated database %q -> %q: %w", targetPath, finalPath, err)
	}
	consolidationOK = true

	for _, path := range localDBs {
		if err := removeDBArtifacts(path, logger); err != nil {
			return "", err
		}
	}
	cleanupFetchedFiles(fetched, logger)

	if err := saveRemoteState(statePath, nextState); err != nil {
		return "", err
	}

	logger.Printf("Consolidated database: %s", finalPath)
	return finalPath, nil
}

func ensureSympathDir(dir string, logger verboseLogger) error {
	_, statErr := os.Stat(dir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create sympath directory %q: %w", dir, err)
	}
	if errors.Is(statErr, os.ErrNotExist) {
		logger.Printf("Created database directory: %s", dir)
	}
	return nil
}

func ensureSeededRemotesFile(dir string, logger verboseLogger) error {
	path := filepath.Join(dir, remotesFileName)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat remotes file %q: %w", path, err)
	}

	if err := os.WriteFile(path, []byte(remotesFileTemplate), 0644); err != nil {
		return fmt.Errorf("seed remotes file %q: %w", path, err)
	}
	logger.Printf("Seeded remotes configuration file: %s", path)
	return nil
}

func ensureMachineIdentity(dir string, logger verboseLogger) (inventory.MachineIdentity, error) {
	path := filepath.Join(dir, machineIDFileName)
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = unknownHostnameName
	}

	data, err := os.ReadFile(path)
	if err == nil {
		machineID := strings.TrimSpace(string(data))
		if machineID == "" {
			return inventory.MachineIdentity{}, fmt.Errorf("machine ID file %q is empty", path)
		}
		return inventory.MachineIdentity{MachineID: machineID, Hostname: hostname}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return inventory.MachineIdentity{}, fmt.Errorf("read machine ID file %q: %w", path, err)
	}

	machineID := uuid.NewString()
	if err := os.WriteFile(path, []byte(machineID+"\n"), 0644); err != nil {
		return inventory.MachineIdentity{}, fmt.Errorf("write machine ID file %q: %w", path, err)
	}
	logger.Printf("Created machine ID file: %s", path)

	return inventory.MachineIdentity{
		MachineID: machineID,
		Hostname:  hostname,
	}, nil
}

func loadRemoteTargets(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read remotes file %q: %w", path, err)
	}

	var remotes []string
	seen := make(map[string]struct{})
	lines := strings.Split(string(data), "\n")
	for i, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 1 {
			return nil, fmt.Errorf("invalid remotes entry on line %d in %q: expected exactly one SSH target", i+1, path)
		}
		target := fields[0]
		if strings.HasPrefix(target, "-") || strings.ContainsAny(target, "/\\") {
			return nil, fmt.Errorf("invalid remotes entry on line %d in %q: %q is not a valid SSH target token", i+1, path, target)
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		remotes = append(remotes, target)
	}
	return remotes, nil
}

func loadRemoteState(path string) (remoteState, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return remoteState{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read remote state file %q: %w", path, err)
	}
	var state remoteState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parse remote state file %q: %w", path, err)
	}
	if state == nil {
		return remoteState{}, nil
	}
	return state, nil
}

func saveRemoteState(path string, state remoteState) error {
	normalized := make(remoteState, len(state))
	for alias, machineIDs := range state {
		sorted := append([]string(nil), machineIDs...)
		sort.Strings(sorted)
		normalized[alias] = uniqueStrings(sorted)
	}
	data, err := json.MarshalIndent(normalized, "", "  ")
	if err != nil {
		return fmt.Errorf("encode remote state file %q: %w", path, err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write remote state file %q: %w", path, err)
	}
	return nil
}

func fetchRemotes(
	ctx context.Context,
	dir string,
	remotes []string,
	transport remoteTransport,
	logger verboseLogger,
) ([]fetchedRemote, map[string]struct{}, error) {
	var fetched []fetchedRemote
	succeeded := make(map[string]struct{})
	for _, alias := range remotes {
		logger.Printf("Fetching remote database from %s", alias)
		remotePath, err := transport.LocateRemoteDB(ctx, alias)
		if err != nil {
			logger.Warnf("Remote %s skipped: %v", alias, err)
			continue
		}
		localPath, err := newTempDBPath(dir, fetchTempPrefix)
		if err != nil {
			cleanupFetchedFiles(fetched, logger)
			return nil, nil, err
		}
		if err := transport.FetchRemoteDB(ctx, alias, remotePath, localPath); err != nil {
			removeDBArtifacts(localPath, logger)
			logger.Warnf("Remote %s fetch failed: %v", alias, err)
			continue
		}
		fetched = append(fetched, fetchedRemote{Alias: alias, Path: localPath})
		succeeded[alias] = struct{}{}
	}
	return fetched, succeeded, nil
}

func importLocalDB(
	ctx context.Context,
	target *sql.DB,
	path string,
	identity inventory.MachineIdentity,
	logger verboseLogger,
) error {
	logger.Printf("Reading local database fragment: %s", path)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open database %q: %w", path, err)
	}
	defer db.Close()

	if err := inventory.PrepareLocalMachineDB(ctx, db, identity); err != nil {
		return fmt.Errorf("prepare local database %q: %w", path, err)
	}
	if _, err := inventory.ImportCurrentScans(ctx, target, db); err != nil {
		return fmt.Errorf("import local database %q: %w", path, err)
	}
	return nil
}

func importFetchedRemote(ctx context.Context, target *sql.DB, path string, logger verboseLogger) (inventory.ImportSummary, error) {
	logger.Printf("Reading fetched remote database: %s", path)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return inventory.ImportSummary{}, fmt.Errorf("open fetched remote database %q: %w", path, err)
	}
	defer db.Close()

	if err := inventory.ConfigureConnection(ctx, db); err != nil {
		return inventory.ImportSummary{}, fmt.Errorf("configure fetched remote database %q: %w", path, err)
	}
	if err := validateImportableRemoteDB(ctx, db, path); err != nil {
		return inventory.ImportSummary{}, err
	}

	summary, err := inventory.ImportCurrentScans(ctx, target, db)
	if err != nil {
		return inventory.ImportSummary{}, fmt.Errorf("import fetched remote database %q: %w", path, err)
	}
	return summary, nil
}

func validateImportableRemoteDB(ctx context.Context, db *sql.DB, path string) error {
	ok, err := inventory.IsMachineAwareInventoryDB(ctx, db)
	if err != nil {
		return fmt.Errorf("validate database %q: %w", path, err)
	}
	if !ok {
		return fmt.Errorf("database %q was created before machine-aware consolidation; run sympath once on that remote machine to upgrade it, then retry", path)
	}
	return nil
}

func listSympathDBs(dir string) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.sympath"))
	if err != nil {
		return nil, fmt.Errorf("find .sympath files in %q: %w", dir, err)
	}
	filtered := paths[:0]
	for _, path := range paths {
		base := filepath.Base(path)
		if strings.HasPrefix(base, buildTempPrefix) || strings.HasPrefix(base, fetchTempPrefix) {
			continue
		}
		filtered = append(filtered, path)
	}
	sort.Strings(filtered)
	return filtered, nil
}

func newTempDBPath(dir, prefix string) (string, error) {
	filename, err := inventory.NewRandomSympathFilename()
	if err != nil {
		return "", fmt.Errorf("generate temporary database filename: %w", err)
	}
	return filepath.Join(dir, prefix+filename), nil
}

func newFinalDBPath(dir string) (string, error) {
	filename, err := inventory.NewRandomSympathFilename()
	if err != nil {
		return "", fmt.Errorf("generate final database filename: %w", err)
	}
	return filepath.Join(dir, filename), nil
}

func machineIDsForRemovedAliases(state remoteState, current []string) []string {
	currentSet := make(map[string]struct{}, len(current))
	for _, alias := range current {
		currentSet[alias] = struct{}{}
	}
	var machineIDs []string
	for alias, ids := range state {
		if _, ok := currentSet[alias]; ok {
			continue
		}
		machineIDs = append(machineIDs, ids...)
	}
	sort.Strings(machineIDs)
	return uniqueStrings(machineIDs)
}

func cloneRemoteState(in remoteState) remoteState {
	out := make(remoteState, len(in))
	for alias, ids := range in {
		out[alias] = append([]string(nil), ids...)
	}
	return out
}

func cleanupFetchedFiles(fetched []fetchedRemote, logger verboseLogger) {
	for _, remote := range fetched {
		_ = removeDBArtifacts(remote.Path, logger)
	}
}

func difference(left, right []string) []string {
	rightSet := make(map[string]struct{}, len(right))
	for _, id := range right {
		rightSet[id] = struct{}{}
	}
	var out []string
	for _, id := range left {
		if _, ok := rightSet[id]; ok {
			continue
		}
		out = append(out, id)
	}
	sort.Strings(out)
	return uniqueStrings(out)
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := values[:0]
	var prev string
	for i, value := range values {
		if i == 0 || value != prev {
			out = append(out, value)
			prev = value
		}
	}
	return out
}

func removeDBArtifacts(path string, logger verboseLogger) error {
	for _, artifact := range []string{path, path + "-wal", path + "-shm", path + "-journal"} {
		if err := os.Remove(artifact); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %q: %w", artifact, err)
		} else if err == nil {
			logger.Printf("Removed merged artifact: %s", artifact)
		}
	}
	return nil
}

func (shellRemoteTransport) LocateRemoteDB(ctx context.Context, target string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, remoteCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ssh", target, "sh", "-c", `set -- "$HOME"/.sympath/*.sympath; [ -e "$1" ] || exit 1; ls -1t "$HOME"/.sympath/*.sympath 2>/dev/null | head -n 1`)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("locate remote database via ssh: %w", err)
	}

	var path string
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasSuffix(line, ".sympath") {
			path = line
			break
		}
	}
	if path == "" {
		return "", fmt.Errorf("no remote ~/.sympath/*.sympath file found")
	}
	return path, nil
}

func (shellRemoteTransport) FetchRemoteDB(ctx context.Context, target, remotePath, localPath string) error {
	ctx, cancel := context.WithTimeout(ctx, remoteCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "scp", "-q", fmt.Sprintf("%s:%s", target, remotePath), localPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		trimmed := strings.TrimSpace(string(output))
		if trimmed != "" {
			return fmt.Errorf("copy remote database via scp: %w: %s", err, trimmed)
		}
		return fmt.Errorf("copy remote database via scp: %w", err)
	}
	return nil
}
