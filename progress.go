package inventory

import "sync/atomic"

// ScanProgress captures live scan counts that can be sampled while a
// scan is running. The zero value is ready to use.
type ScanProgress struct {
	discovered   atomic.Int64
	processed    atomic.Int64
	hashed       atomic.Int64
	reused       atomic.Int64
	walkComplete atomic.Bool
}

// ScanProgressSnapshot is a point-in-time view of [ScanProgress].
type ScanProgressSnapshot struct {
	FilesDiscovered int64
	FilesProcessed  int64
	FilesPending    int64
	FilesHashed     int64
	FilesReused     int64
	WalkComplete    bool
}

// Snapshot returns a consistent, current view of the scan progress.
func (p *ScanProgress) Snapshot() ScanProgressSnapshot {
	if p == nil {
		return ScanProgressSnapshot{}
	}

	discovered := p.discovered.Load()
	processed := p.processed.Load()
	if processed > discovered {
		processed = discovered
	}

	return ScanProgressSnapshot{
		FilesDiscovered: discovered,
		FilesProcessed:  processed,
		FilesPending:    discovered - processed,
		FilesHashed:     p.hashed.Load(),
		FilesReused:     p.reused.Load(),
		WalkComplete:    p.walkComplete.Load(),
	}
}

func (p *ScanProgress) noteDiscovered(state string) {
	if p == nil {
		return
	}

	p.discovered.Add(1)
	switch state {
	case "reused":
		p.reused.Add(1)
		p.processed.Add(1)
	case "error":
		p.processed.Add(1)
	}
}

func (p *ScanProgress) noteHashed() {
	if p == nil {
		return
	}
	p.hashed.Add(1)
	p.processed.Add(1)
}

func (p *ScanProgress) noteWalkComplete() {
	if p == nil {
		return
	}
	p.walkComplete.Store(true)
}
