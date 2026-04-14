package main

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"

	_ "modernc.org/sqlite"
)

//go:embed ui_static/*
var uiStaticFS embed.FS

// localOnlyTransport is a remoteTransport that always fails, causing
// consolidateSympathDir to skip all remote fetches while still
// consolidating local .sympath files.
type localOnlyTransport struct{}

func (localOnlyTransport) LocateRemoteDB(context.Context, string) (string, error) {
	return "", errors.New("remote fetch disabled in ui mode")
}

func (localOnlyTransport) FetchRemoteDB(context.Context, string, string, string) error {
	return errors.New("remote fetch disabled in ui mode")
}

// openUIReadOnlyDBForRunUI lets tests observe the precise point where the UI
// opens the chosen database file. runUIWithIO must keep startupLock and the
// exclusive DB guard held until this open succeeds so another process cannot
// reconsolidate ~/.sympath and replace the chosen path in the small gap between
// selection and open.
var openUIReadOnlyDBForRunUI = openUIReadOnlyDB

func runUIWithIO(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 {
		return errors.New("ui accepts no arguments")
	}

	dir, err := sympathStateDir()
	if err != nil {
		return fmt.Errorf("resolve sympath state directory: %w", err)
	}

	startupLock, err := acquireScanStartupLock(context.Background(), dir)
	if err != nil {
		return fmt.Errorf("acquire scan startup lock: %w", err)
	}
	defer startupLock.Close()
	dbGuard, err := acquireScanDBGuardLockExclusive(context.Background(), dir)
	if err != nil {
		return fmt.Errorf("acquire exclusive database guard: %w", err)
	}
	defer dbGuard.Close()

	// Suppress warnings from the local-only transport so remote
	// "skipped" messages don't clutter the UI startup output.
	logger := newVerboseLogger(stderr, false)

	if err := ensureSympathDir(dir, logger); err != nil {
		return err
	}
	identity, err := ensureMachineIdentity(dir, logger)
	if err != nil {
		return err
	}
	dbPath, err := consolidateSympathDir(context.Background(), dir, identity, localOnlyTransport{}, logger)
	if err != nil {
		return err
	}
	if err := requireExistingUIDatabase(dbPath); err != nil {
		return err
	}

	// Open the selected database while startupLock and the exclusive DB guard
	// are still held. That closes the last startup race: a scan cannot
	// reconsolidate ~/.sympath and swap out dbPath between "choose this file"
	// and "open this file".
	//
	// Once the read-only connection exists, the UI intentionally releases both
	// locks and serves that already-open database as a best-effort snapshot.
	// Later scans are free to reconsolidate and publish newer databases; this UI
	// process keeps reading the snapshot it selected during startup rather than
	// pinning the shared DB guard for its entire lifetime.
	db, err := openUIReadOnlyDBForRunUI(context.Background(), dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := dbGuard.Close(); err != nil {
		return fmt.Errorf("release exclusive database guard: %w", err)
	}
	dbGuard = nil
	if err := startupLock.Close(); err != nil {
		return fmt.Errorf("release scan startup lock: %w", err)
	}
	startupLock = nil

	srv := &uiServer{db: db, updates: newUpdateChecker()}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", srv.handleStatus)
	mux.HandleFunc("/api/roots", srv.handleRoots)
	mux.HandleFunc("/api/dirs", srv.handleDirs)
	mux.HandleFunc("/api/compare", srv.handleCompare)

	staticSub, err := fs.Sub(uiStaticFS, "ui_static")
	if err != nil {
		return fmt.Errorf("embed sub filesystem: %w", err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticSub)))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	addr := listener.Addr().(*net.TCPAddr)
	url := fmt.Sprintf("http://127.0.0.1:%d", addr.Port)

	normalizedDB, err := normalizeDBPath(dbPath)
	if err != nil {
		normalizedDB = dbPath
	}
	fmt.Fprintf(stderr, "Database: %s\n", normalizedDB)
	fmt.Fprintf(stderr, "Serving UI at %s\n", url)

	_ = openBrowser(url)

	// Graceful shutdown on interrupt.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	httpServer := &http.Server{Handler: mux}
	go func() {
		<-sigCh
		httpServer.Close()
	}()

	if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func requireExistingUIDatabase(dbPath string) error {
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("no inventory database found yet; run `sympath scan [ROOT]` first")
		}
		return fmt.Errorf("stat database: %w", err)
	}
	return nil
}

func openUIReadOnlyDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", readOnlySQLiteDSN(dbPath))
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := configureReadOnlyConnection(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure database: %w", err)
	}
	return db, nil
}

func configureReadOnlyConnection(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000")
	return err
}

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", url).Start()
	case "linux":
		return exec.Command("xdg-open", url).Start()
	case "windows":
		return exec.Command("cmd", "/c", "start", url).Start()
	default:
		return fmt.Errorf("unsupported platform %s", runtime.GOOS)
	}
}
