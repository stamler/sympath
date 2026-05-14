package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestReleaseAssetNameForGOOSArch(t *testing.T) {
	tests := []struct {
		goos, goarch string
		want         string
		wantErr      string
	}{
		{goos: "darwin", goarch: "arm64", want: "sympath-darwin-arm64.tar.gz"},
		{goos: "linux", goarch: "amd64", want: "sympath-linux-amd64.tar.gz"},
		{goos: "windows", goarch: "amd64", want: "sympath-windows-amd64.zip"},
		{goos: "darwin", goarch: "amd64", wantErr: "unsupported platform darwin/amd64"},
	}

	for _, tt := range tests {
		got, err := releaseAssetNameForGOOSArch(tt.goos, tt.goarch)
		if tt.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("%s/%s: expected error containing %q, got %v", tt.goos, tt.goarch, tt.wantErr, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s/%s: unexpected error: %v", tt.goos, tt.goarch, err)
		}
		if got != tt.want {
			t.Fatalf("%s/%s: expected %q, got %q", tt.goos, tt.goarch, tt.want, got)
		}
	}
}

func TestManagedInstallTargetForGOOS_DefaultAndOverride(t *testing.T) {
	got, err := managedInstallTargetForGOOS("linux", "/home/dean", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/home/dean/.local/bin/sympath" {
		t.Fatalf("expected default POSIX target, got %q", got)
	}

	got, err = managedInstallTargetForGOOS("windows", `C:\Users\dean`, `C:\Users\dean\AppData\Local`, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Join(`C:\Users\dean\AppData\Local`, "Programs", "sympath", "bin", "sympath.exe") {
		t.Fatalf("expected default Windows target, got %q", got)
	}

	got, err = managedInstallTargetForGOOS("linux", "/home/dean", "", "/custom/bin")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/custom/bin/sympath" {
		t.Fatalf("expected override target, got %q", got)
	}
}

func TestReleaseInstallerTemporaryUpdateParentAvoidsDefaultTmp(t *testing.T) {
	home := t.TempDir()
	installer := releaseInstaller{
		goos:    "linux",
		homeDir: func() (string, error) { return home, nil },
	}

	t.Setenv("TMPDIR", "")
	got, err := installer.temporaryUpdateParent()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Join(home, ".cache") {
		t.Fatalf("expected home cache when TMPDIR is empty, got %q", got)
	}

	t.Setenv("TMPDIR", "/tmp")
	got, err = installer.temporaryUpdateParent()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Join(home, ".cache") {
		t.Fatalf("expected home cache when TMPDIR is /tmp, got %q", got)
	}

	customTmp := filepath.Join(t.TempDir(), "tmp")
	t.Setenv("TMPDIR", customTmp)
	got, err = installer.temporaryUpdateParent()
	if err != nil {
		t.Fatal(err)
	}
	if got != customTmp {
		t.Fatalf("expected custom TMPDIR to be preserved, got %q", got)
	}
}

func TestReleaseInstallerVerifyManagedInstallTargetRejectsUnmanagedExecutable(t *testing.T) {
	home := t.TempDir()
	targetPath, err := managedInstallTargetForGOOS("linux", home, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, []byte("managed"), 0755); err != nil {
		t.Fatal(err)
	}

	installer := releaseInstaller{
		goos:           "linux",
		goarch:         "amd64",
		homeDir:        func() (string, error) { return home, nil },
		executablePath: func() (string, error) { return filepath.Join(t.TempDir(), "sympath"), nil },
		localAppData:   func() string { return "" },
	}

	_, err = installer.verifyManagedInstallTarget()
	if err == nil {
		t.Fatal("expected unmanaged executable to be rejected")
	}
	if !strings.Contains(err.Error(), "supports managed installs") {
		t.Fatalf("expected managed-install error, got %v", err)
	}
}

func TestReleaseInstallerInstallReleaseUsesHomeCacheTempDirAndCleansUp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX install test only runs on POSIX hosts")
	}

	assetName, err := releaseAssetNameForGOOSArch(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Skip(err.Error())
	}

	assetDir := t.TempDir()
	writePosixReleaseFixtureForTest(t, assetDir, assetName, "v1.2.4")

	home := t.TempDir()
	targetPath, err := managedInstallTargetForGOOS(runtime.GOOS, home, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, []byte("#!/bin/sh\nprintf 'old\\n'\n"), 0755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("TMPDIR", "")
	t.Setenv(installBaseDirEnv, assetDir)
	installer := releaseInstaller{
		goos:           runtime.GOOS,
		goarch:         runtime.GOARCH,
		homeDir:        func() (string, error) { return home, nil },
		executablePath: func() (string, error) { return targetPath, nil },
		localAppData:   func() string { return "" },
	}

	if _, err := installer.installRelease(context.Background(), "v1.2.4"); err != nil {
		t.Fatal(err)
	}

	cacheDir := filepath.Join(home, ".cache")
	if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
		t.Fatalf("expected temporary cache directory to be removed, got %v", err)
	}
}

func TestReleaseInstallerInstallReleaseChecksumMismatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("checksum fixture uses a POSIX shell script binary")
	}

	assetDir := t.TempDir()
	assetName := "sympath-linux-amd64.tar.gz"
	writePosixReleaseFixtureForTest(t, assetDir, assetName, "v1.2.4")
	if err := os.WriteFile(filepath.Join(assetDir, "checksums.txt"), []byte("deadbeef  "+assetName+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	home := t.TempDir()
	targetPath, err := managedInstallTargetForGOOS("linux", home, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, []byte("#!/bin/sh\nprintf 'old\\n'\n"), 0755); err != nil {
		t.Fatal(err)
	}

	t.Setenv(installBaseDirEnv, assetDir)
	installer := releaseInstaller{
		goos:           "linux",
		goarch:         "amd64",
		homeDir:        func() (string, error) { return home, nil },
		executablePath: func() (string, error) { return targetPath, nil },
		localAppData:   func() string { return "" },
		runVersion: func(context.Context, string) (string, error) {
			t.Fatal("expected checksum failure before version probe")
			return "", nil
		},
	}

	_, err = installer.installRelease(context.Background(), "v1.2.4")
	if err == nil {
		t.Fatal("expected checksum mismatch to fail")
	}
	if !strings.Contains(err.Error(), "checksum verification failed") {
		t.Fatalf("expected checksum failure, got %v", err)
	}
}

func TestReleaseInstallerInstallReleaseHintsWhenTemporaryBinaryCannotRun(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX install test only runs on POSIX hosts")
	}

	assetName, err := releaseAssetNameForGOOSArch(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Skip(err.Error())
	}

	assetDir := t.TempDir()
	writePosixReleaseFixtureForTest(t, assetDir, assetName, "v1.2.4")

	home := t.TempDir()
	targetPath, err := managedInstallTargetForGOOS(runtime.GOOS, home, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, []byte("#!/bin/sh\nprintf 'old\\n'\n"), 0755); err != nil {
		t.Fatal(err)
	}

	t.Setenv(installBaseDirEnv, assetDir)
	installer := releaseInstaller{
		goos:           runtime.GOOS,
		goarch:         runtime.GOARCH,
		homeDir:        func() (string, error) { return home, nil },
		executablePath: func() (string, error) { return targetPath, nil },
		localAppData:   func() string { return "" },
		runVersion: func(context.Context, string) (string, error) {
			return "", os.ErrPermission
		},
	}

	_, err = installer.installRelease(context.Background(), "v1.2.4")
	if err == nil {
		t.Fatal("expected temporary binary version probe to fail")
	}
	errText := err.Error()
	if !strings.Contains(errText, "read downloaded binary version") {
		t.Fatalf("expected version probe failure, got %v", err)
	}
	if !strings.Contains(errText, `TMPDIR="$HOME/.cache" sympath update`) {
		t.Fatalf("expected TMPDIR update hint, got %v", err)
	}
}

func TestReleaseInstallerInstallReleasePOSIX(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX install test only runs on POSIX hosts")
	}

	assetName, err := releaseAssetNameForGOOSArch(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Skip(err.Error())
	}

	assetDir := t.TempDir()
	writePosixReleaseFixtureForTest(t, assetDir, assetName, "v1.2.4")

	home := t.TempDir()
	targetPath, err := managedInstallTargetForGOOS(runtime.GOOS, home, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, []byte("#!/bin/sh\nprintf 'old\\n'\n"), 0755); err != nil {
		t.Fatal(err)
	}

	t.Setenv(installBaseDirEnv, assetDir)
	installer := releaseInstaller{
		goos:           runtime.GOOS,
		goarch:         runtime.GOARCH,
		homeDir:        func() (string, error) { return home, nil },
		executablePath: func() (string, error) { return targetPath, nil },
		localAppData:   func() string { return "" },
	}

	result, err := installer.installRelease(context.Background(), "v1.2.4")
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != "v1.2.4" {
		t.Fatalf("expected installed version v1.2.4, got %+v", result)
	}
	if result.TargetPath != targetPath {
		t.Fatalf("expected target path %q, got %+v", targetPath, result)
	}

	versionOut, err := exec.Command(targetPath, "version").CombinedOutput()
	if err != nil {
		t.Fatalf("expected installed binary to run, got %v: %s", err, versionOut)
	}
	if got := strings.TrimSpace(string(versionOut)); got != "v1.2.4" {
		t.Fatalf("expected installed version v1.2.4, got %q", got)
	}
}

func TestRunBinaryVersionSuppressesInternalUpdateNotice(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture only runs on POSIX hosts")
	}

	binaryPath := filepath.Join(t.TempDir(), "sympath")
	script := "#!/bin/sh\nif [ \"${" + internalNoUpdateEnv + ":-}\" != \"1\" ]; then\n  printf 'missing internal env\\n' >&2\n  exit 7\nfi\nprintf 'v1.2.4\\n'\nprintf 'update notice\\n' >&2\n"
	if err := os.WriteFile(binaryPath, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}

	versionOut, err := runBinaryVersion(context.Background(), binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if versionOut != "v1.2.4" {
		t.Fatalf("expected clean version output, got %q", versionOut)
	}
}

func TestReleaseInstallerInstallReleaseWindowsSchedulesHelper(t *testing.T) {
	assetDir := t.TempDir()
	writeWindowsReleaseFixtureForTest(t, assetDir, "sympath-windows-amd64.zip", []byte("windows-binary"))

	home := t.TempDir()
	localAppData := filepath.Join(t.TempDir(), "localappdata")
	targetPath, err := managedInstallTargetForGOOS("windows", home, localAppData, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, []byte("old-windows-binary"), 0755); err != nil {
		t.Fatal(err)
	}

	var capturedName string
	var capturedArgs []string
	t.Setenv(installBaseDirEnv, assetDir)
	installer := releaseInstaller{
		goos:             "windows",
		goarch:           "amd64",
		homeDir:          func() (string, error) { return home, nil },
		executablePath:   func() (string, error) { return targetPath, nil },
		localAppData:     func() string { return localAppData },
		runVersion:       func(context.Context, string) (string, error) { return "v1.2.4", nil },
		currentPID:       func() int { return 4242 },
		powerShellBinary: "powershell",
		startProcess: func(name string, args []string) error {
			capturedName = name
			capturedArgs = append([]string{}, args...)
			return nil
		},
	}

	result, err := installer.installRelease(context.Background(), "v1.2.4")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Scheduled {
		t.Fatalf("expected Windows install to be scheduled, got %+v", result)
	}
	if capturedName != "powershell" {
		t.Fatalf("expected powershell helper, got %q", capturedName)
	}

	commandLine := strings.Join(capturedArgs, " ")
	if !strings.Contains(commandLine, "Wait-Process -Id 4242") {
		t.Fatalf("expected helper to wait for current pid, got %q", commandLine)
	}
	if !strings.Contains(commandLine, targetPath) {
		t.Fatalf("expected helper command to include target path, got %q", commandLine)
	}
	if !strings.Contains(commandLine, "Copy-Item") {
		t.Fatalf("expected helper command to copy staged executable, got %q", commandLine)
	}
}

func TestBuildWindowsReplaceCommandQuotesSingleQuotes(t *testing.T) {
	name, args := buildWindowsReplaceCommand("powershell", 7, `C:\tmp\sympath'new.exe`, `C:\bin\sympath.exe`)
	if name != "powershell" {
		t.Fatalf("expected powershell name, got %q", name)
	}
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "Wait-Process -Id 7") {
		t.Fatalf("expected wait command, got %q", joined)
	}
	if !strings.Contains(joined, "sympath''new.exe") {
		t.Fatalf("expected single quotes to be escaped, got %q", joined)
	}
}

func writePosixReleaseFixtureForTest(t *testing.T, dir, assetName, version string) {
	t.Helper()

	binary := []byte("#!/bin/sh\nif [ \"$1\" = \"version\" ]; then\n  printf '%s\\n' \"" + version + "\"\n  exit 0\nfi\nprintf 'sympath stub\\n'\n")
	archivePath := filepath.Join(dir, assetName)
	writeTarGzForTest(t, archivePath, "sympath", binary, 0755)
	writeChecksumsForTest(t, dir, archivePath)
}

func writeWindowsReleaseFixtureForTest(t *testing.T, dir, assetName string, data []byte) {
	t.Helper()

	archivePath := filepath.Join(dir, assetName)
	writeZipForTest(t, archivePath, "sympath.exe", data)
	writeChecksumsForTest(t, dir, archivePath)
}

func writeChecksumsForTest(t *testing.T, dir string, assets ...string) {
	t.Helper()

	var buf bytes.Buffer
	for _, asset := range assets {
		data, err := os.ReadFile(asset)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		buf.WriteString(hex.EncodeToString(sum[:]))
		buf.WriteString("  ")
		buf.WriteString(filepath.Base(asset))
		buf.WriteByte('\n')
	}

	if err := os.WriteFile(filepath.Join(dir, "checksums.txt"), buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

func writeTarGzForTest(t *testing.T, archivePath, name string, data []byte, mode int64) {
	t.Helper()

	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	gz := gzip.NewWriter(file)
	defer gz.Close()

	writer := tar.NewWriter(gz)
	defer writer.Close()

	if err := writer.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
}

func writeZipForTest(t *testing.T, archivePath, name string, data []byte) {
	t.Helper()

	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	writer := zip.NewWriter(file)
	defer writer.Close()

	entry, err := writer.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(entry, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}
