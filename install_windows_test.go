//go:build windows

package inventory_test

import (
	"archive/zip"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallPowerShell_InstallsAndRepairsUserPath(t *testing.T) {
	if os.Getenv("RUN_INSTALLER_SMOKE_TESTS") != "1" {
		t.Skip("set RUN_INSTALLER_SMOKE_TESTS=1 to run installer smoke tests")
	}

	repoRoot := repoRoot(t)
	assetDir := t.TempDir()
	assetName := "sympath-windows-amd64.zip"
	zipPath := filepath.Join(assetDir, assetName)
	exePath := buildWindowsStubBinary(t, "v3.4.5")
	writeZipFile(t, zipPath, exePath, "sympath.exe")
	writeChecksums(t, assetDir, zipPath)

	home := filepath.Join(t.TempDir(), "home")
	localAppData := filepath.Join(t.TempDir(), "localappdata")
	if err := os.MkdirAll(home, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(localAppData, 0755); err != nil {
		t.Fatal(err)
	}
	installDir := filepath.Join(localAppData, "Programs", "sympath", "bin")
	oldBinDir := filepath.Join(t.TempDir(), "oldbin")
	if err := os.MkdirAll(oldBinDir, 0755); err != nil {
		t.Fatal(err)
	}
	oldExePath := buildWindowsStubBinary(t, "old")
	if err := copyFile(oldExePath, filepath.Join(oldBinDir, "sympath.exe")); err != nil {
		t.Fatal(err)
	}

	wrapper := fmt.Sprintf(`$ErrorActionPreference = 'Stop'
$originalUserPath = [Environment]::GetEnvironmentVariable('PATH', 'User')
try {
  $env:SYMPATH_INSTALL_BASE_DIR = %s
  $env:LOCALAPPDATA = %s
  $env:USERPROFILE = %s
  $env:HOME = %s
  [Environment]::SetEnvironmentVariable('PATH', (%s + ';' + %s), 'User')
  $env:PATH = (%s + ';' + %s + ';' + $env:PATH)
  & %s
  $first = sympath version
  & %s
  $second = sympath version
  $resolved = (Get-Command sympath -ErrorAction Stop).Source
  $userPath = [Environment]::GetEnvironmentVariable('PATH', 'User')
  Write-Host ('FIRST=' + $first.Trim())
  Write-Host ('SECOND=' + $second.Trim())
  Write-Host ('RESOLVED=' + $resolved)
  Write-Host ('USERPATH=' + $userPath)
} finally {
  [Environment]::SetEnvironmentVariable('PATH', $originalUserPath, 'User')
}
`,
		psSingleQuoted(assetDir),
		psSingleQuoted(localAppData),
		psSingleQuoted(home),
		psSingleQuoted(home),
		psSingleQuoted(oldBinDir),
		psSingleQuoted(installDir),
		psSingleQuoted(oldBinDir),
		psSingleQuoted(installDir),
		psSingleQuoted(filepath.Join(repoRoot, "install.ps1")),
		psSingleQuoted(filepath.Join(repoRoot, "install.ps1")),
	)

	wrapperPath := filepath.Join(t.TempDir(), "run-install.ps1")
	if err := os.WriteFile(wrapperPath, []byte(wrapper), 0644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", wrapperPath)
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("PowerShell installer test failed: %v\n%s", err, output)
	}

	out := string(output)
	if !strings.Contains(out, "FIRST=v3.4.5") || !strings.Contains(out, "SECOND=v3.4.5") {
		t.Fatalf("expected installed command to report v3.4.5 twice, got:\n%s", out)
	}
	if !strings.Contains(out, "already installed") {
		t.Fatalf("expected idempotent reinstall output, got:\n%s", out)
	}
	if resolved := findOutputValue(t, out, "RESOLVED="); resolved != filepath.Join(installDir, "sympath.exe") {
		t.Fatalf("expected installed binary to win PATH resolution, got %q\nfull output:\n%s", resolved, out)
	}

	userPath := findOutputValue(t, out, "USERPATH=")
	if !strings.HasPrefix(strings.ToLower(userPath), strings.ToLower(installDir+";")) && !strings.EqualFold(userPath, installDir) {
		t.Fatalf("expected user PATH to start with install dir, got %q\nfull output:\n%s", userPath, out)
	}
	if strings.Count(strings.ToLower(userPath), strings.ToLower(installDir)) != 1 {
		t.Fatalf("expected install dir to appear once in user PATH, got %q\nfull output:\n%s", userPath, out)
	}

	target := filepath.Join(installDir, "sympath.exe")
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("expected installed binary at %q, got %v", target, err)
	}
}

func buildWindowsStubBinary(t *testing.T, version string) string {
	t.Helper()

	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "main.go")
	exePath := filepath.Join(tempDir, "sympath.exe")
	source := fmt.Sprintf(`package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(%q)
		return
	}
	fmt.Println("sympath stub")
}
`, version)
	if err := os.WriteFile(sourcePath, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "build", "-o", exePath, sourcePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("building windows stub failed: %v\n%s", err, output)
	}
	return exePath
}

func writeZipFile(t *testing.T, archivePath, sourcePath, name string) {
	t.Helper()

	sourceData, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}

	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	zw := zip.NewWriter(file)
	defer zw.Close()

	entry, err := zw.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write(sourceData); err != nil {
		t.Fatal(err)
	}
}

func copyFile(sourcePath, destPath string) error {
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}
	return os.WriteFile(destPath, data, 0755)
}

func findOutputValue(t *testing.T, output, prefix string) string {
	t.Helper()

	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	t.Fatalf("expected output line starting with %q, got:\n%s", prefix, output)
	return ""
}

func psSingleQuoted(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
