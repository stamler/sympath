package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	defaultReleaseRepo = "stamler/sympath"

	installRepoEnv       = "SYMPATH_INSTALL_REPO"
	installVersionEnv    = "SYMPATH_INSTALL_VERSION"
	installDirEnv        = "SYMPATH_INSTALL_DIR"
	installBaseDirEnv    = "SYMPATH_INSTALL_BASE_DIR"
	installBaseURLEnv    = "SYMPATH_INSTALL_BASE_URL"
	installLocalAppData  = "LOCALAPPDATA"
	updateInstallTimeout = 2 * time.Minute
	internalNoUpdateEnv  = "SYMPATH_INTERNAL_NO_UPDATE_NOTICE"
)

type installedRelease struct {
	Version    string
	TargetPath string
	ReleaseURL string
	Scheduled  bool
}

type releaseInstaller struct {
	goos             string
	goarch           string
	homeDir          func() (string, error)
	executablePath   func() (string, error)
	download         func(context.Context, string, string) error
	runVersion       func(context.Context, string) (string, error)
	startProcess     func(string, []string) error
	localAppData     func() string
	currentPID       func() int
	powerShellBinary string
}

var newReleaseInstaller = func() releaseInstaller {
	return releaseInstaller{}.withDefaults()
}

var installManagedRelease = func(ctx context.Context, targetVersion string) (installedRelease, error) {
	return newReleaseInstaller().installRelease(ctx, targetVersion)
}

func (i releaseInstaller) withDefaults() releaseInstaller {
	if i.goos == "" {
		i.goos = runtime.GOOS
	}
	if i.goarch == "" {
		i.goarch = runtime.GOARCH
	}
	if i.homeDir == nil {
		i.homeDir = sympathHomeDir
	}
	if i.executablePath == nil {
		i.executablePath = os.Executable
	}
	if i.download == nil {
		i.download = downloadReleaseSource
	}
	if i.runVersion == nil {
		i.runVersion = runBinaryVersion
	}
	if i.startProcess == nil {
		i.startProcess = startDetachedProcess
	}
	if i.localAppData == nil {
		i.localAppData = func() string { return strings.TrimSpace(os.Getenv(installLocalAppData)) }
	}
	if i.currentPID == nil {
		i.currentPID = os.Getpid
	}
	if i.powerShellBinary == "" {
		i.powerShellBinary = findPowerShellBinary()
	}
	return i
}

func (i releaseInstaller) installRelease(ctx context.Context, targetVersion string) (installedRelease, error) {
	i = i.withDefaults()

	targetVersion = strings.TrimSpace(targetVersion)
	if targetVersion == "" {
		targetVersion = requestedInstallVersion()
	}
	if targetVersion == "" {
		return installedRelease{}, fmt.Errorf("no release version selected for installation")
	}

	targetPath, err := i.verifyManagedInstallTarget()
	if err != nil {
		return installedRelease{}, err
	}

	assetName, err := releaseAssetNameForGOOSArch(i.goos, i.goarch)
	if err != nil {
		return installedRelease{}, err
	}

	tempRoot, err := os.MkdirTemp("", "sympath-update-")
	if err != nil {
		return installedRelease{}, fmt.Errorf("create temporary update directory: %w", err)
	}
	defer os.RemoveAll(tempRoot)

	archivePath := filepath.Join(tempRoot, assetName)
	checksumsPath := filepath.Join(tempRoot, "checksums.txt")
	extractedDir := filepath.Join(tempRoot, "extracted")

	if err := os.MkdirAll(extractedDir, 0755); err != nil {
		return installedRelease{}, fmt.Errorf("create extraction directory: %w", err)
	}

	if err := i.download(ctx, releaseAssetSource(assetName, targetVersion), archivePath); err != nil {
		return installedRelease{}, fmt.Errorf("download release asset: %w", err)
	}
	if err := i.download(ctx, releaseAssetSource("checksums.txt", targetVersion), checksumsPath); err != nil {
		return installedRelease{}, fmt.Errorf("download checksums: %w", err)
	}

	expectedChecksum, err := expectedChecksum(checksumsPath, assetName)
	if err != nil {
		return installedRelease{}, err
	}
	actualChecksum, err := computeSHA256Hex(archivePath)
	if err != nil {
		return installedRelease{}, err
	}
	if actualChecksum != expectedChecksum {
		return installedRelease{}, fmt.Errorf("checksum verification failed for %s", assetName)
	}

	extractedBinary, err := extractReleaseBinary(archivePath, assetName, extractedDir, executableNameForGOOS(i.goos))
	if err != nil {
		return installedRelease{}, err
	}

	downloadedVersion, err := i.runVersion(ctx, extractedBinary)
	if err != nil {
		return installedRelease{}, fmt.Errorf("read downloaded binary version: %w", err)
	}

	result := installedRelease{
		Version:    downloadedVersion,
		TargetPath: targetPath,
		ReleaseURL: releasePageURL(targetVersion),
	}

	if i.goos == "windows" {
		if err := i.scheduleWindowsInstall(extractedBinary, targetPath); err != nil {
			return installedRelease{}, err
		}
		result.Scheduled = true
		return result, nil
	}

	if err := installPOSIXBinary(extractedBinary, targetPath); err != nil {
		return installedRelease{}, err
	}

	return result, nil
}

func (i releaseInstaller) verifyManagedInstallTarget() (string, error) {
	targetPath, err := i.managedInstallTarget()
	if err != nil {
		return "", err
	}

	if _, err := os.Stat(targetPath); err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("sympath update only supports managed installs at %s; reinstall with: %s", targetPath, reinstallCommandForGOOS(i.goos))
		}
		return "", fmt.Errorf("stat managed install target: %w", err)
	}

	currentExecutable, err := i.executablePath()
	if err != nil {
		return "", fmt.Errorf("resolve current executable: %w", err)
	}

	if !sameExecutablePath(currentExecutable, targetPath, i.goos) {
		return "", fmt.Errorf("sympath update only supports managed installs at %s; current executable is %s. Reinstall with: %s", targetPath, currentExecutable, reinstallCommandForGOOS(i.goos))
	}

	return targetPath, nil
}

func (i releaseInstaller) managedInstallTarget() (string, error) {
	home, err := i.homeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return managedInstallTargetForGOOS(i.goos, home, i.localAppData(), strings.TrimSpace(os.Getenv(installDirEnv)))
}

func (i releaseInstaller) scheduleWindowsInstall(extractedBinary, targetPath string) error {
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("ensure managed install directory: %w", err)
	}

	stageFile, err := os.CreateTemp(dir, "sympath-update-*.exe")
	if err != nil {
		return fmt.Errorf("create staged update file: %w", err)
	}
	stagePath := stageFile.Name()
	if err := stageFile.Close(); err != nil {
		os.Remove(stagePath)
		return fmt.Errorf("close staged update file: %w", err)
	}

	if err := copyFileContents(extractedBinary, stagePath, 0755); err != nil {
		os.Remove(stagePath)
		return fmt.Errorf("stage updated executable: %w", err)
	}
	if err := os.Chmod(stagePath, 0755); err != nil {
		os.Remove(stagePath)
		return fmt.Errorf("mark staged update executable: %w", err)
	}

	name, args := buildWindowsReplaceCommand(i.powerShellBinary, i.currentPID(), stagePath, targetPath)
	if err := i.startProcess(name, args); err != nil {
		os.Remove(stagePath)
		return fmt.Errorf("launch Windows update helper: %w", err)
	}

	return nil
}

func releaseRepoSlug() string {
	if repo := strings.TrimSpace(os.Getenv(installRepoEnv)); repo != "" {
		return repo
	}
	return defaultReleaseRepo
}

func latestReleaseAPIURL() string {
	return fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", releaseRepoSlug())
}

func releasePageURL(version string) string {
	return fmt.Sprintf("https://github.com/%s/releases/tag/%s", releaseRepoSlug(), version)
}

func reinstallCommandForGOOS(goos string) string {
	switch goos {
	case "windows":
		return fmt.Sprintf("irm https://raw.githubusercontent.com/%s/main/install.ps1 | iex", releaseRepoSlug())
	default:
		return fmt.Sprintf("curl -fsSL https://raw.githubusercontent.com/%s/main/install.sh | sh", releaseRepoSlug())
	}
}

func requestedInstallVersion() string {
	return strings.TrimSpace(os.Getenv(installVersionEnv))
}

func releaseAssetSource(asset, version string) string {
	if baseDir := strings.TrimSpace(os.Getenv(installBaseDirEnv)); baseDir != "" {
		return filepath.Join(baseDir, asset)
	}

	if baseURL := strings.TrimSpace(os.Getenv(installBaseURLEnv)); baseURL != "" {
		return strings.TrimRight(baseURL, "/") + "/" + asset
	}

	if version == "" {
		version = requestedInstallVersion()
	}

	repo := releaseRepoSlug()
	if version != "" {
		return fmt.Sprintf("https://github.com/%s/releases/download/%s/%s", repo, version, asset)
	}
	return fmt.Sprintf("https://github.com/%s/releases/latest/download/%s", repo, asset)
}

func releaseAssetNameForGOOSArch(goos, goarch string) (string, error) {
	switch {
	case goos == "darwin" && goarch == "arm64":
		return "sympath-darwin-arm64.tar.gz", nil
	case goos == "linux" && goarch == "amd64":
		return "sympath-linux-amd64.tar.gz", nil
	case goos == "windows" && goarch == "amd64":
		return "sympath-windows-amd64.zip", nil
	case goos == "darwin":
		return "", fmt.Errorf("unsupported platform %s/%s; the updater currently supports macOS arm64 and Linux amd64", goos, goarch)
	case goos == "linux":
		return "", fmt.Errorf("unsupported platform %s/%s; the updater currently supports Linux amd64 only", goos, goarch)
	case goos == "windows":
		return "", fmt.Errorf("unsupported platform %s/%s; the updater currently supports Windows amd64 only", goos, goarch)
	default:
		return "", fmt.Errorf("unsupported platform %s/%s", goos, goarch)
	}
}

func executableNameForGOOS(goos string) string {
	if goos == "windows" {
		return "sympath.exe"
	}
	return "sympath"
}

func managedInstallTargetForGOOS(goos, home, localAppData, installDirOverride string) (string, error) {
	if installDirOverride != "" {
		return filepath.Join(installDirOverride, executableNameForGOOS(goos)), nil
	}

	switch goos {
	case "windows":
		if localAppData == "" {
			localAppData = filepath.Join(home, "AppData", "Local")
		}
		if strings.TrimSpace(localAppData) == "" {
			return "", fmt.Errorf("LOCALAPPDATA is empty")
		}
		return filepath.Join(localAppData, "Programs", "sympath", "bin", executableNameForGOOS(goos)), nil
	default:
		return filepath.Join(home, ".local", "bin", executableNameForGOOS(goos)), nil
	}
}

func sameExecutablePath(left, right, goos string) bool {
	normalizedLeft := normalizeExecutablePath(left)
	normalizedRight := normalizeExecutablePath(right)
	if goos == "windows" {
		return strings.EqualFold(normalizedLeft, normalizedRight)
	}
	return normalizedLeft == normalizedRight
}

func normalizeExecutablePath(value string) string {
	cleaned := filepath.Clean(value)
	if abs, err := filepath.Abs(cleaned); err == nil {
		cleaned = abs
	}
	if resolved, err := filepath.EvalSymlinks(cleaned); err == nil {
		cleaned = resolved
	}
	return cleaned
}

func downloadReleaseSource(ctx context.Context, source, dest string) error {
	if strings.TrimSpace(os.Getenv(installBaseDirEnv)) != "" {
		return copyFileContents(source, dest, 0644)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "sympath update")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download request returned %s", resp.Status)
	}

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func copyFileContents(source, dest string, mode os.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func expectedChecksum(checksumsPath, assetName string) (string, error) {
	data, err := os.ReadFile(checksumsPath)
	if err != nil {
		return "", fmt.Errorf("read checksums: %w", err)
	}

	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == assetName {
			return strings.ToLower(fields[0]), nil
		}
	}

	return "", fmt.Errorf("no checksum found for %s", assetName)
}

func computeSHA256Hex(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s for checksum: %w", path, err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func extractReleaseBinary(archivePath, assetName, destDir, binaryName string) (string, error) {
	switch {
	case strings.HasSuffix(assetName, ".tar.gz"):
		return extractTarGzBinary(archivePath, destDir, binaryName)
	case strings.HasSuffix(assetName, ".zip"):
		return extractZipBinary(archivePath, destDir, binaryName)
	default:
		return "", fmt.Errorf("unsupported archive format for %s", assetName)
	}
}

func extractTarGzBinary(archivePath, destDir, binaryName string) (string, error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return "", fmt.Errorf("open archive %s: %w", archivePath, err)
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return "", fmt.Errorf("open gzip archive %s: %w", archivePath, err)
	}
	defer gz.Close()

	reader := tar.NewReader(gz)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("read tar archive %s: %w", archivePath, err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		if path.Base(header.Name) != binaryName {
			continue
		}

		destPath := filepath.Join(destDir, binaryName)
		out, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
		if err != nil {
			return "", fmt.Errorf("create extracted binary %s: %w", destPath, err)
		}
		if _, err := io.Copy(out, reader); err != nil {
			out.Close()
			return "", fmt.Errorf("extract binary %s: %w", binaryName, err)
		}
		if err := out.Close(); err != nil {
			return "", fmt.Errorf("close extracted binary %s: %w", destPath, err)
		}
		return destPath, nil
	}

	return "", fmt.Errorf("archive %s did not contain %s", archivePath, binaryName)
}

func extractZipBinary(archivePath, destDir, binaryName string) (string, error) {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return "", fmt.Errorf("open zip archive %s: %w", archivePath, err)
	}
	defer reader.Close()

	for _, file := range reader.File {
		if path.Base(file.Name) != binaryName {
			continue
		}

		rc, err := file.Open()
		if err != nil {
			return "", fmt.Errorf("open zipped binary %s: %w", binaryName, err)
		}

		destPath := filepath.Join(destDir, binaryName)
		out, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
		if err != nil {
			rc.Close()
			return "", fmt.Errorf("create extracted binary %s: %w", destPath, err)
		}
		if _, err := io.Copy(out, rc); err != nil {
			rc.Close()
			out.Close()
			return "", fmt.Errorf("extract binary %s: %w", binaryName, err)
		}
		if err := rc.Close(); err != nil {
			out.Close()
			return "", fmt.Errorf("close zipped binary %s: %w", binaryName, err)
		}
		if err := out.Close(); err != nil {
			return "", fmt.Errorf("close extracted binary %s: %w", destPath, err)
		}
		return destPath, nil
	}

	return "", fmt.Errorf("archive %s did not contain %s", archivePath, binaryName)
}

func runBinaryVersion(ctx context.Context, binaryPath string) (string, error) {
	cmd := exec.CommandContext(ctx, binaryPath, "version")
	cmd.Env = append(os.Environ(), internalNoUpdateEnv+"=1")

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return "", err
	}

	versionLine, _, _ := strings.Cut(string(output), "\n")
	versionLine = strings.TrimSpace(versionLine)
	if versionLine == "" {
		return "", fmt.Errorf("version output was empty")
	}
	return versionLine, nil
}

func installPOSIXBinary(sourcePath, targetPath string) error {
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("ensure managed install directory: %w", err)
	}

	tempFile, err := os.CreateTemp(dir, ".sympath-update-*")
	if err != nil {
		return fmt.Errorf("create temporary target file: %w", err)
	}
	tempPath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("close temporary target file: %w", err)
	}

	if err := copyFileContents(sourcePath, tempPath, 0755); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("stage updated executable: %w", err)
	}
	if err := os.Chmod(tempPath, 0755); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("mark staged executable: %w", err)
	}

	if err := os.Rename(tempPath, targetPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("replace managed executable: %w", err)
	}

	return nil
}

func findPowerShellBinary() string {
	if path, err := exec.LookPath("powershell"); err == nil {
		return path
	}
	if path, err := exec.LookPath("pwsh"); err == nil {
		return path
	}
	return "powershell"
}

func buildWindowsReplaceCommand(powerShellBinary string, pid int, stagePath, targetPath string) (string, []string) {
	script := strings.Join([]string{
		"$ErrorActionPreference = 'Stop'",
		fmt.Sprintf("Wait-Process -Id %d", pid),
		"Start-Sleep -Milliseconds 250",
		fmt.Sprintf("Copy-Item -Path %s -Destination %s -Force", powerShellSingleQuoted(stagePath), powerShellSingleQuoted(targetPath)),
		fmt.Sprintf("Remove-Item -Path %s -Force", powerShellSingleQuoted(stagePath)),
	}, "; ")

	return powerShellBinary, []string{
		"-NoProfile",
		"-NonInteractive",
		"-ExecutionPolicy",
		"Bypass",
		"-Command",
		script,
	}
}

func powerShellSingleQuoted(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
