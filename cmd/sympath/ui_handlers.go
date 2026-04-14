package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"path"
	"sort"
)

type uiServer struct {
	db      *sql.DB
	updates updateChecker
}

type rootEntry struct {
	MachineID string `json:"machine_id"`
	Hostname  string `json:"hostname"`
	Root      string `json:"root"`
}

func (s *uiServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	checker := s.updates.withDefaults()

	ctx, cancel := context.WithTimeout(r.Context(), updateCheckTimeout)
	defer cancel()

	status, err := checker.resolveStatus(ctx, version, false)
	if err != nil {
		// The UI still renders current version details even when the
		// live refresh is unavailable, so surface the status payload.
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *uiServer) handleRoots(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `
		SELECT r.machine_id, s.hostname, r.root
		FROM roots r
		JOIN scans s ON s.scan_id = r.current_scan_id
		ORDER BY s.hostname, r.root
	`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var roots []rootEntry
	for rows.Next() {
		var e rootEntry
		if err := rows.Scan(&e.MachineID, &e.Hostname, &e.Root); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		roots = append(roots, e)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if roots == nil {
		roots = []rootEntry{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(roots)
}

func (s *uiServer) handleDirs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	machineID := q.Get("machine_id")
	root := q.Get("root")

	if machineID == "" || root == "" {
		http.Error(w, "machine_id and root are required", http.StatusBadRequest)
		return
	}

	scanID, err := resolveScanID(r.Context(), s.db, machineID, root)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows, err := s.db.QueryContext(r.Context(),
		"SELECT rel_path FROM entries WHERE scan_id = ?", scanID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	dirSet := make(map[string]struct{})
	for rows.Next() {
		var relPath string
		if err := rows.Scan(&relPath); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Collect every ancestor directory of this file.
		dir := path.Dir(relPath)
		for dir != "." && dir != "" {
			if _, ok := dirSet[dir]; ok {
				break // already have this and all its parents
			}
			dirSet[dir] = struct{}{}
			dir = path.Dir(dir)
		}
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dirs := make([]string, 0, len(dirSet))
	for d := range dirSet {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dirs)
}

func (s *uiServer) handleCompare(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	leftMachine := q.Get("left_machine")
	leftRoot := q.Get("left_root")
	rightMachine := q.Get("right_machine")
	rightRoot := q.Get("right_root")
	leftPrefix := q.Get("left_prefix")
	rightPrefix := q.Get("right_prefix")
	byContent := q.Get("by_content") == "1"

	if leftMachine == "" || leftRoot == "" || rightMachine == "" || rightRoot == "" {
		http.Error(w, "left_machine, left_root, right_machine, and right_root are required", http.StatusBadRequest)
		return
	}

	result, err := compareRoots(r.Context(), s.db, leftMachine, leftRoot, rightMachine, rightRoot, leftPrefix, rightPrefix, byContent)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
