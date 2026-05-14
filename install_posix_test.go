package inventory_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
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

func TestInstallScript_InstallsAndRepairsShellPath(t *testing.T) {
	if os.Getenv("RUN_INSTALLER_SMOKE_TESTS") != "1" {
		t.Skip("set RUN_INSTALLER_SMOKE_TESTS=1 to run installer smoke tests")
	}
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer test only runs on POSIX hosts")
	}

	repoRoot := repoRoot(t)
	assetDir := t.TempDir()
	assetName := currentPOSIXAssetName(t)
	writePosixReleaseFixture(t, assetDir, assetName, "v1.2.3")

	home := t.TempDir()
	firstOutput := runPOSIXInstaller(t, repoRoot, home, assetDir, nil)

	target := filepath.Join(home, ".local", "bin", "sympath")
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("expected installed binary at %q, got %v", target, err)
	}

	versionOut, err := exec.Command(target, "version").CombinedOutput()
	if err != nil {
		t.Fatalf("expected installed binary to run, got %v: %s", err, versionOut)
	}
	if got := strings.TrimSpace(string(versionOut)); got != "v1.2.3" {
		t.Fatalf("expected installed version v1.2.3, got %q", got)
	}

	profilePath := filepath.Join(home, ".zshrc")
	profileData, err := os.ReadFile(profilePath)
	if err != nil {
		t.Fatalf("expected zsh profile to be updated, got %v", err)
	}
	profileText := string(profileData)
	if strings.Count(profileText, "# >>> sympath install >>>") != 1 {
		t.Fatalf("expected one managed PATH block, got:\n%s", profileText)
	}
	if !strings.Contains(profileText, "export PATH=\""+filepath.Join(home, ".local", "bin")+":$PATH\"") {
		t.Fatalf("expected PATH export for install dir, got:\n%s", profileText)
	}
	if !strings.Contains(firstOutput, "Installed sympath v1.2.3") {
		t.Fatalf("expected install output, got:\n%s", firstOutput)
	}

	secondOutput := runPOSIXInstaller(t, repoRoot, home, assetDir, nil)
	if !strings.Contains(secondOutput, "already installed") {
		t.Fatalf("expected idempotent reinstall output, got:\n%s", secondOutput)
	}

	profileData, err = os.ReadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(profileData), "# >>> sympath install >>>") != 1 {
		t.Fatalf("expected PATH block to stay deduplicated, got:\n%s", profileData)
	}
}

func TestInstallScript_UsesHomeCacheTempDirAndCleansUp(t *testing.T) {
	if os.Getenv("RUN_INSTALLER_SMOKE_TESTS") != "1" {
		t.Skip("set RUN_INSTALLER_SMOKE_TESTS=1 to run installer smoke tests")
	}
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer test only runs on POSIX hosts")
	}

	repoRoot := repoRoot(t)
	assetDir := t.TempDir()
	writePosixReleaseFixture(t, assetDir, currentPOSIXAssetName(t), "v1.2.3")

	home := t.TempDir()
	runPOSIXInstaller(t, repoRoot, home, assetDir, []string{"TMPDIR="})

	cacheDir := filepath.Join(home, ".cache")
	if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
		t.Fatalf("expected temporary cache directory to be removed, got %v", err)
	}
}

func TestInstallScript_IgnoresVersionProbeStderrOnSuccess(t *testing.T) {
	if os.Getenv("RUN_INSTALLER_SMOKE_TESTS") != "1" {
		t.Skip("set RUN_INSTALLER_SMOKE_TESTS=1 to run installer smoke tests")
	}
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer test only runs on POSIX hosts")
	}

	repoRoot := repoRoot(t)
	assetDir := t.TempDir()
	assetName := currentPOSIXAssetName(t)
	binary := []byte("#!/bin/sh\nprintf 'v1.2.3\\n'\nprintf 'update available\\n' >&2\n")
	archivePath := filepath.Join(assetDir, assetName)
	writeTarGz(t, archivePath, "sympath", binary, 0755)
	writeChecksums(t, assetDir, archivePath)

	home := t.TempDir()
	output := runPOSIXInstaller(t, repoRoot, home, assetDir, []string{"TMPDIR=/tmp"})

	if !strings.Contains(output, "Installed sympath v1.2.3 to ") {
		t.Fatalf("expected clean installed version, got:\n%s", output)
	}
	if strings.Contains(output, "update available") {
		t.Fatalf("expected version probe stderr to stay out of installer output, got:\n%s", output)
	}
}

func TestInstallScript_WarnsWhenRunAsRoot(t *testing.T) {
	if os.Getenv("RUN_INSTALLER_SMOKE_TESTS") != "1" {
		t.Skip("set RUN_INSTALLER_SMOKE_TESTS=1 to run installer smoke tests")
	}
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer test only runs on POSIX hosts")
	}

	repoRoot := repoRoot(t)
	assetDir := t.TempDir()
	writePosixReleaseFixture(t, assetDir, currentPOSIXAssetName(t), "v2.0.0")

	home := t.TempDir()
	output := runPOSIXInstaller(t, repoRoot, home, assetDir, []string{"SYMPATH_INSTALL_TEST_EUID=0"})
	if !strings.Contains(output, "running as root installs sympath only for the root account") {
		t.Fatalf("expected root warning, got:\n%s", output)
	}
}

func TestInstallScript_HintsWhenExtractedBinaryIsNotExecutable(t *testing.T) {
	if os.Getenv("RUN_INSTALLER_SMOKE_TESTS") != "1" {
		t.Skip("set RUN_INSTALLER_SMOKE_TESTS=1 to run installer smoke tests")
	}
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer test only runs on POSIX hosts")
	}

	repoRoot := repoRoot(t)
	assetDir := t.TempDir()
	assetName := currentPOSIXAssetName(t)
	binary := []byte("#!/bin/sh\nprintf 'v1.2.3\\n'\n")
	archivePath := filepath.Join(assetDir, assetName)
	writeTarGz(t, archivePath, "sympath", binary, 0644)
	writeChecksums(t, assetDir, archivePath)

	home := t.TempDir()
	output := runPOSIXInstallerExpectError(t, repoRoot, home, assetDir, nil)

	if !strings.Contains(output, "downloaded archive contained sympath, but it is not executable after extraction") {
		t.Fatalf("expected executable extraction failure, got:\n%s", output)
	}
	if !strings.Contains(output, `TMPDIR="$HOME/.cache" sh`) {
		t.Fatalf("expected TMPDIR retry hint, got:\n%s", output)
	}
}

func TestInstallScript_HintsWhenDownloadedBinaryVersionCannotRun(t *testing.T) {
	if os.Getenv("RUN_INSTALLER_SMOKE_TESTS") != "1" {
		t.Skip("set RUN_INSTALLER_SMOKE_TESTS=1 to run installer smoke tests")
	}
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer test only runs on POSIX hosts")
	}

	repoRoot := repoRoot(t)
	assetDir := t.TempDir()
	assetName := currentPOSIXAssetName(t)
	binary := []byte("#!/bin/sh\nprintf 'permission denied\\n' >&2\nexit 126\n")
	archivePath := filepath.Join(assetDir, assetName)
	writeTarGz(t, archivePath, "sympath", binary, 0755)
	writeChecksums(t, assetDir, archivePath)

	home := t.TempDir()
	output := runPOSIXInstallerExpectError(t, repoRoot, home, assetDir, []string{"TMPDIR=/tmp"})

	if !strings.Contains(output, "read downloaded binary version") {
		t.Fatalf("expected version probe failure, got:\n%s", output)
	}
	if !strings.Contains(output, `TMPDIR="$HOME/.cache" sh`) {
		t.Fatalf("expected TMPDIR retry hint, got:\n%s", output)
	}
}

func TestInstallScript_DoesNotClaimCurrentShellWhenOlderBinaryWins(t *testing.T) {
	if os.Getenv("RUN_INSTALLER_SMOKE_TESTS") != "1" {
		t.Skip("set RUN_INSTALLER_SMOKE_TESTS=1 to run installer smoke tests")
	}
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer test only runs on POSIX hosts")
	}

	repoRoot := repoRoot(t)
	assetDir := t.TempDir()
	writePosixReleaseFixture(t, assetDir, currentPOSIXAssetName(t), "v2.1.0")

	home := t.TempDir()
	oldBinDir := filepath.Join(t.TempDir(), "oldbin")
	if err := os.MkdirAll(oldBinDir, 0755); err != nil {
		t.Fatal(err)
	}
	oldBinary := filepath.Join(oldBinDir, "sympath")
	if err := os.WriteFile(oldBinary, []byte("#!/bin/sh\nprintf 'old\\n'\n"), 0755); err != nil {
		t.Fatal(err)
	}

	installDir := filepath.Join(home, ".local", "bin")
	pathValue := oldBinDir + string(os.PathListSeparator) + installDir + string(os.PathListSeparator) + os.Getenv("PATH")
	output := runPOSIXInstaller(t, repoRoot, home, assetDir, []string{"PATH=" + pathValue})

	if strings.Contains(output, "sympath is available now:") {
		t.Fatalf("expected installer not to claim immediate availability when an older binary wins PATH resolution, got:\n%s", output)
	}
	if !strings.Contains(output, "Open a new shell or run:") {
		t.Fatalf("expected shell refresh guidance, got:\n%s", output)
	}
}

func TestPackageReleaseScript_ProducesExpectedAssets(t *testing.T) {
	if os.Getenv("RUN_INSTALLER_SMOKE_TESTS") != "1" {
		t.Skip("set RUN_INSTALLER_SMOKE_TESTS=1 to run packaging smoke tests")
	}
	if runtime.GOOS == "windows" {
		t.Skip("release packaging smoke test only runs on POSIX hosts")
	}

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is required for release packaging test")
	}

	repoRoot := repoRoot(t)
	distDir := t.TempDir()
	buildDir := filepath.Join(distDir, "build")
	cacheDir := filepath.Join(t.TempDir(), "gocache")
	cmd := exec.Command("bash", filepath.Join(repoRoot, "scripts", "package-release.sh"))
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"DIST_DIR="+distDir,
		"BUILD_DIR="+buildDir,
		"GOCACHE="+cacheDir,
		"VERSION=v4.5.6",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("package-release.sh failed: %v\n%s", err, output)
	}

	expected := []string{
		filepath.Join(distDir, "sympath-darwin-arm64.tar.gz"),
		filepath.Join(distDir, "sympath-linux-amd64.tar.gz"),
		filepath.Join(distDir, "sympath-windows-amd64.zip"),
		filepath.Join(distDir, "checksums.txt"),
	}
	for _, path := range expected {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected release artifact %q, got %v", path, err)
		}
	}

	checksums, err := os.ReadFile(filepath.Join(distDir, "checksums.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"sympath-darwin-arm64.tar.gz",
		"sympath-linux-amd64.tar.gz",
		"sympath-windows-amd64.zip",
	} {
		if !strings.Contains(string(checksums), name) {
			t.Fatalf("expected checksums.txt to include %s, got:\n%s", name, checksums)
		}
	}

	assertTarGzContains(t, filepath.Join(distDir, "sympath-darwin-arm64.tar.gz"), "sympath")
	assertTarGzContains(t, filepath.Join(distDir, "sympath-linux-amd64.tar.gz"), "sympath")
	assertZipContains(t, filepath.Join(distDir, "sympath-windows-amd64.zip"), "sympath.exe")
}

func runPOSIXInstaller(t *testing.T, repoRoot, home, assetDir string, extraEnv []string) string {
	t.Helper()

	cmd := exec.Command("/bin/sh", filepath.Join(repoRoot, "install.sh"))
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"HOME="+home,
		"PATH="+os.Getenv("PATH"),
		"SHELL=/bin/zsh",
		"SYMPATH_INSTALL_BASE_DIR="+assetDir,
	)
	cmd.Env = append(cmd.Env, extraEnv...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh failed: %v\n%s", err, output)
	}
	return string(output)
}

func runPOSIXInstallerExpectError(t *testing.T, repoRoot, home, assetDir string, extraEnv []string) string {
	t.Helper()

	cmd := exec.Command("/bin/sh", filepath.Join(repoRoot, "install.sh"))
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"HOME="+home,
		"PATH="+os.Getenv("PATH"),
		"SHELL=/bin/zsh",
		"SYMPATH_INSTALL_BASE_DIR="+assetDir,
	)
	cmd.Env = append(cmd.Env, extraEnv...)

	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected install.sh to fail, got:\n%s", output)
	}
	return string(output)
}

func writePosixReleaseFixture(t *testing.T, dir, assetName, version string) {
	t.Helper()

	binary := []byte("#!/bin/sh\nif [ \"$1\" = \"version\" ]; then\n  printf '%s\\n' \"" + version + "\"\n  exit 0\nfi\nprintf 'sympath stub\\n'\n")
	archivePath := filepath.Join(dir, assetName)
	writeTarGz(t, archivePath, "sympath", binary, 0755)
	writeChecksums(t, dir, archivePath)
}

func writeChecksums(t *testing.T, dir string, assets ...string) {
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

func writeTarGz(t *testing.T, archivePath, name string, data []byte, mode int64) {
	t.Helper()

	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	gz := gzip.NewWriter(file)
	defer gz.Close()

	tw := tar.NewWriter(gz)
	defer tw.Close()

	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: mode,
		Size: int64(len(data)),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatal(err)
	}
}

func assertTarGzContains(t *testing.T, archivePath, want string) {
	t.Helper()

	file, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	header, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if header.Name != want {
		t.Fatalf("expected %q inside %s, got %q", want, archivePath, header.Name)
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Fatalf("expected exactly one file inside %s", archivePath)
	}
}

func assertZipContains(t *testing.T, archivePath, want string) {
	t.Helper()

	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()

	if len(zr.File) != 1 {
		t.Fatalf("expected one file inside %s, got %d", archivePath, len(zr.File))
	}
	if zr.File[0].Name != want {
		t.Fatalf("expected %q inside %s, got %q", want, archivePath, zr.File[0].Name)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func currentPOSIXAssetName(t *testing.T) string {
	t.Helper()

	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "darwin/arm64":
		return "sympath-darwin-arm64.tar.gz"
	case "linux/amd64":
		return "sympath-linux-amd64.tar.gz"
	default:
		t.Fatalf("unsupported smoke-test platform %s/%s", runtime.GOOS, runtime.GOARCH)
		return ""
	}
}
