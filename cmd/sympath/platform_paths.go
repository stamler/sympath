package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// sympathHomeDir prefers HOME when present so tests and CI can redirect
// sympath's per-user state directory consistently on every platform,
// including Windows runners where os.UserHomeDir may ignore HOME.
func sympathHomeDir() (string, error) {
	if home := strings.TrimSpace(os.Getenv("HOME")); home != "" {
		return home, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("home directory is empty")
	}
	return home, nil
}

func sympathStateDir() (string, error) {
	home, err := sympathHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".sympath"), nil
}

func readOnlySQLiteDSN(dbPath string) string {
	return readOnlySQLiteDSNForGOOS(dbPath, runtime.GOOS)
}

func readOnlySQLiteDSNForGOOS(dbPath, goos string) string {
	path := strings.ReplaceAll(dbPath, `\`, `/`)

	if goos == "windows" {
		return "file:///" + strings.TrimPrefix(path, "/") + "?mode=ro"
	}

	return "file:" + path + "?mode=ro"
}
