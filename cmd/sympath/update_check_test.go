package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestStatusFromReleaseDetectsNewerVersion(t *testing.T) {
	current, err := parseTaggedVersion("v1.2.3")
	if err != nil {
		t.Fatal(err)
	}

	status, err := statusFromRelease("v1.2.3", current, latestRelease{
		Version: "v1.3.0",
		URL:     "https://example.com/releases/v1.3.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !status.UpdateAvailable {
		t.Fatalf("expected update to be available, got %+v", status)
	}
}

func TestStatusFromReleaseSameVersionIsCurrent(t *testing.T) {
	current, err := parseTaggedVersion("v1.2.3")
	if err != nil {
		t.Fatal(err)
	}

	status, err := statusFromRelease("v1.2.3", current, latestRelease{
		Version: "v1.2.3",
		URL:     "https://example.com/releases/v1.2.3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.UpdateAvailable {
		t.Fatalf("expected no update to be available, got %+v", status)
	}
}

func TestBaseUpdateStatusRejectsUnsupportedBuild(t *testing.T) {
	status, _, supported := baseUpdateStatus("dev")
	if supported {
		t.Fatalf("expected dev build to be unsupported, got %+v", status)
	}
	if status.Supported {
		t.Fatalf("expected Supported=false, got %+v", status)
	}
}

func TestUpdateCheckerResolveStatusUsesFreshCache(t *testing.T) {
	now := time.Date(2026, time.April, 14, 10, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	checker := updateChecker{
		stateDir: func() (string, error) { return dir, nil },
		now:      func() time.Time { return now },
		fetchLatest: func(context.Context) (latestRelease, error) {
			t.Fatal("expected fresh cache to avoid live fetch")
			return latestRelease{}, nil
		},
	}

	if err := checker.writeCache(updateCache{
		CheckedAt:     now.Add(-time.Hour),
		LatestVersion: "v1.2.4",
		ReleaseURL:    "https://example.com/releases/v1.2.4",
	}); err != nil {
		t.Fatal(err)
	}

	status, err := checker.resolveStatus(context.Background(), "v1.2.3", false)
	if err != nil {
		t.Fatal(err)
	}
	if !status.UpdateAvailable || status.LatestVersion != "v1.2.4" {
		t.Fatalf("expected cached update status, got %+v", status)
	}
}

func TestUpdateCheckerResolveStatusRefreshesStaleCache(t *testing.T) {
	now := time.Date(2026, time.April, 14, 10, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	checker := updateChecker{
		stateDir: func() (string, error) { return dir, nil },
		now:      func() time.Time { return now },
		fetchLatest: func(context.Context) (latestRelease, error) {
			return latestRelease{
				Version: "v1.2.5",
				URL:     "https://example.com/releases/v1.2.5",
			}, nil
		},
	}

	if err := checker.writeCache(updateCache{
		CheckedAt:     now.Add(-25 * time.Hour),
		LatestVersion: "v1.2.4",
		ReleaseURL:    "https://example.com/releases/v1.2.4",
	}); err != nil {
		t.Fatal(err)
	}

	status, err := checker.resolveStatus(context.Background(), "v1.2.3", false)
	if err != nil {
		t.Fatal(err)
	}
	if status.LatestVersion != "v1.2.5" {
		t.Fatalf("expected stale cache to refresh, got %+v", status)
	}

	cache, ok, err := checker.readCache()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected refreshed cache to be written")
	}
	if cache.LatestVersion != "v1.2.5" {
		t.Fatalf("expected refreshed cache version v1.2.5, got %+v", cache)
	}
}

func TestUpdateCheckerResolveStatusForceBypassesFreshCache(t *testing.T) {
	now := time.Date(2026, time.April, 14, 10, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	fetches := 0
	checker := updateChecker{
		stateDir: func() (string, error) { return dir, nil },
		now:      func() time.Time { return now },
		fetchLatest: func(context.Context) (latestRelease, error) {
			fetches++
			return latestRelease{
				Version: "v1.2.6",
				URL:     "https://example.com/releases/v1.2.6",
			}, nil
		},
	}

	if err := checker.writeCache(updateCache{
		CheckedAt:     now.Add(-time.Hour),
		LatestVersion: "v1.2.4",
		ReleaseURL:    "https://example.com/releases/v1.2.4",
	}); err != nil {
		t.Fatal(err)
	}

	status, err := checker.resolveStatus(context.Background(), "v1.2.3", true)
	if err != nil {
		t.Fatal(err)
	}
	if fetches != 1 {
		t.Fatalf("expected force refresh to call live fetch once, got %d", fetches)
	}
	if status.LatestVersion != "v1.2.6" {
		t.Fatalf("expected force refresh result v1.2.6, got %+v", status)
	}
}

func TestUpdateCheckerResolveStatusUnavailableOnFetchFailure(t *testing.T) {
	checker := updateChecker{
		stateDir: func() (string, error) { return t.TempDir(), nil },
		now:      time.Now,
		fetchLatest: func(context.Context) (latestRelease, error) {
			return latestRelease{}, errors.New("boom")
		},
	}

	status, err := checker.resolveStatus(context.Background(), "v1.2.3", true)
	if err == nil {
		t.Fatal("expected forced live check to fail")
	}
	if !status.Unavailable {
		t.Fatalf("expected unavailable status on fetch failure, got %+v", status)
	}
}

func TestUpdateCheckerCachePathUsesStateDir(t *testing.T) {
	dir := t.TempDir()
	checker := updateChecker{
		stateDir: func() (string, error) { return dir, nil },
	}

	cachePath, err := checker.cachePath()
	if err != nil {
		t.Fatal(err)
	}
	if cachePath != filepath.Join(dir, updateCheckCacheName) {
		t.Fatalf("expected cache path inside state dir, got %q", cachePath)
	}
}
