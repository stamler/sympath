package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	updateCheckCacheName = "update-check.json"
	updateCheckTTL       = 24 * time.Hour
	updateCheckTimeout   = 2 * time.Second
)

type latestRelease struct {
	Version string
	URL     string
}

type updateCache struct {
	CheckedAt     time.Time `json:"checked_at"`
	LatestVersion string    `json:"latest_version"`
	ReleaseURL    string    `json:"release_url"`
}

type updateStatus struct {
	CurrentVersion  string `json:"current_version"`
	Supported       bool   `json:"supported"`
	LatestVersion   string `json:"latest_version,omitempty"`
	ReleaseURL      string `json:"release_url,omitempty"`
	UpdateAvailable bool   `json:"update_available"`
	Unavailable     bool   `json:"unavailable"`
}

type updateChecker struct {
	stateDir    func() (string, error)
	fetchLatest func(context.Context) (latestRelease, error)
	now         func() time.Time
	ttl         time.Duration
}

type autoUpdateCheck struct {
	cached *updateStatus
	async  <-chan updateCheckResult
}

type updateCheckResult struct {
	status updateStatus
	err    error
}

type taggedVersion struct {
	major int
	minor int
	patch int
}

var newUpdateChecker = func() updateChecker {
	return updateChecker{
		stateDir:    sympathStateDir,
		fetchLatest: fetchLatestGitHubRelease,
		now:         time.Now,
		ttl:         updateCheckTTL,
	}
}

func (c updateChecker) withDefaults() updateChecker {
	if c.stateDir == nil {
		c.stateDir = sympathStateDir
	}
	if c.fetchLatest == nil {
		c.fetchLatest = fetchLatestGitHubRelease
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.ttl <= 0 {
		c.ttl = updateCheckTTL
	}
	return c
}

func (c updateChecker) resolveStatus(ctx context.Context, currentVersion string, force bool) (updateStatus, error) {
	c = c.withDefaults()

	baseStatus, currentTagged, supported := baseUpdateStatus(currentVersion)
	if !supported {
		return baseStatus, nil
	}

	if !force {
		if cached, fresh, err := c.readFreshStatus(currentVersion, currentTagged); err == nil && fresh {
			return cached, nil
		}
	}

	live, err := c.liveStatus(ctx, currentVersion, currentTagged)
	if err != nil {
		baseStatus.Unavailable = true
		return baseStatus, err
	}
	return live, nil
}

func (c updateChecker) startAutoCheck(currentVersion string) *autoUpdateCheck {
	c = c.withDefaults()

	baseStatus, currentTagged, supported := baseUpdateStatus(currentVersion)
	if !supported {
		return &autoUpdateCheck{cached: &baseStatus}
	}

	if cached, fresh, err := c.readFreshStatus(currentVersion, currentTagged); err == nil && fresh {
		return &autoUpdateCheck{cached: &cached}
	}

	resultCh := make(chan updateCheckResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), updateCheckTimeout)
		defer cancel()

		status, err := c.liveStatus(ctx, currentVersion, currentTagged)
		resultCh <- updateCheckResult{status: status, err: err}
	}()

	return &autoUpdateCheck{async: resultCh}
}

func (h *autoUpdateCheck) notice() string {
	if h == nil {
		return ""
	}
	if h.cached != nil {
		return formatAutoUpdateNotice(*h.cached)
	}
	if h.async != nil {
		select {
		case result := <-h.async:
			if result.err == nil {
				return formatAutoUpdateNotice(result.status)
			}
		default:
		}
	}
	return ""
}

func (c updateChecker) readFreshStatus(currentVersion string, currentTagged taggedVersion) (updateStatus, bool, error) {
	cache, ok, err := c.readCache()
	if err != nil || !ok {
		return updateStatus{}, false, err
	}
	if cache.CheckedAt.IsZero() || c.now().Sub(cache.CheckedAt) > c.ttl {
		return updateStatus{}, false, nil
	}

	status, err := statusFromRelease(currentVersion, currentTagged, latestRelease{
		Version: cache.LatestVersion,
		URL:     cache.ReleaseURL,
	})
	if err != nil {
		return updateStatus{}, false, err
	}
	return status, true, nil
}

func (c updateChecker) liveStatus(ctx context.Context, currentVersion string, currentTagged taggedVersion) (updateStatus, error) {
	release, err := c.fetchLatest(ctx)
	if err != nil {
		return updateStatus{}, err
	}

	status, err := statusFromRelease(currentVersion, currentTagged, release)
	if err != nil {
		return updateStatus{}, err
	}

	_ = c.writeCache(updateCache{
		CheckedAt:     c.now().UTC(),
		LatestVersion: release.Version,
		ReleaseURL:    release.URL,
	})

	return status, nil
}

func (c updateChecker) readCache() (updateCache, bool, error) {
	cachePath, err := c.cachePath()
	if err != nil {
		return updateCache{}, false, err
	}

	data, err := os.ReadFile(cachePath)
	if err != nil {
		if os.IsNotExist(err) {
			return updateCache{}, false, nil
		}
		return updateCache{}, false, err
	}

	var cache updateCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return updateCache{}, false, err
	}
	return cache, true, nil
}

func (c updateChecker) writeCache(cache updateCache) error {
	cachePath, err := c.cachePath()
	if err != nil {
		return err
	}

	dir := filepath.Dir(cachePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmpFile, err := os.CreateTemp(dir, "update-check-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, cachePath)
}

func (c updateChecker) cachePath() (string, error) {
	dir, err := c.stateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, updateCheckCacheName), nil
}

func baseUpdateStatus(currentVersion string) (updateStatus, taggedVersion, bool) {
	status := updateStatus{CurrentVersion: currentVersion}

	tagged, err := parseTaggedVersion(currentVersion)
	if err != nil {
		return status, taggedVersion{}, false
	}

	status.Supported = true
	return status, tagged, true
}

func statusFromRelease(currentVersion string, currentTagged taggedVersion, release latestRelease) (updateStatus, error) {
	if strings.TrimSpace(release.Version) == "" {
		return updateStatus{}, fmt.Errorf("latest release version is empty")
	}
	if strings.TrimSpace(release.URL) == "" {
		return updateStatus{}, fmt.Errorf("latest release URL is empty")
	}

	latestTagged, err := parseTaggedVersion(release.Version)
	if err != nil {
		return updateStatus{}, fmt.Errorf("parse latest release version %q: %w", release.Version, err)
	}

	return updateStatus{
		CurrentVersion:  currentVersion,
		Supported:       true,
		LatestVersion:   release.Version,
		ReleaseURL:      release.URL,
		UpdateAvailable: currentTagged.lessThan(latestTagged),
	}, nil
}

func parseTaggedVersion(value string) (taggedVersion, error) {
	if !strings.HasPrefix(value, "v") {
		return taggedVersion{}, fmt.Errorf("version must start with v")
	}

	parts := strings.Split(value[1:], ".")
	if len(parts) != 3 {
		return taggedVersion{}, fmt.Errorf("version must have major.minor.patch")
	}

	major, err := parseVersionPart(parts[0])
	if err != nil {
		return taggedVersion{}, err
	}
	minor, err := parseVersionPart(parts[1])
	if err != nil {
		return taggedVersion{}, err
	}
	patch, err := parseVersionPart(parts[2])
	if err != nil {
		return taggedVersion{}, err
	}

	return taggedVersion{major: major, minor: minor, patch: patch}, nil
}

func parseVersionPart(value string) (int, error) {
	if value == "" {
		return 0, fmt.Errorf("version part is empty")
	}
	part, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("version part %q is not numeric", value)
	}
	if part < 0 {
		return 0, fmt.Errorf("version part %q must not be negative", value)
	}
	return part, nil
}

func (v taggedVersion) lessThan(other taggedVersion) bool {
	if v.major != other.major {
		return v.major < other.major
	}
	if v.minor != other.minor {
		return v.minor < other.minor
	}
	return v.patch < other.patch
}

func fetchLatestGitHubRelease(ctx context.Context) (latestRelease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, latestReleaseAPIURL(), nil)
	if err != nil {
		return latestRelease{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "sympath update-check")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return latestRelease{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return latestRelease{}, fmt.Errorf("latest release request returned %s", resp.Status)
	}

	var payload struct {
		TagName string `json:"tag_name"`
		HTMLURL string `json:"html_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return latestRelease{}, err
	}

	return latestRelease{
		Version: payload.TagName,
		URL:     payload.HTMLURL,
	}, nil
}

func formatAutoUpdateNotice(status updateStatus) string {
	if !status.UpdateAvailable {
		return ""
	}
	return fmt.Sprintf("Update available: %s (current %s)", status.LatestVersion, status.CurrentVersion)
}

func formatUpdateCheckMessage(status updateStatus) string {
	if !status.Supported {
		return fmt.Sprintf("Update checks are unavailable for build %s", status.CurrentVersion)
	}
	if status.UpdateAvailable {
		return fmt.Sprintf("Update available: %s (current %s) %s", status.LatestVersion, status.CurrentVersion, status.ReleaseURL)
	}
	return fmt.Sprintf("%s is up to date", status.CurrentVersion)
}
