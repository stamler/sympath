package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	inventory "sympath"

	"github.com/mattn/go-isatty"
)

const (
	progressTickInterval = 120 * time.Millisecond
	progressTrackWidth   = 20
)

var progressSpinnerFrames = []rune{'|', '/', '-', '\\'}

func newScanProgressDisplay(w io.Writer) (*inventory.ScanProgress, *scanProgressDisplay) {
	if !supportsLiveProgress(w) {
		return nil, nil
	}

	progress := &inventory.ScanProgress{}
	display := &scanProgressDisplay{
		w:        w,
		progress: progress,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	return progress, display
}

func supportsLiveProgress(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fd := f.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}

type scanProgressDisplay struct {
	w        io.Writer
	progress *inventory.ScanProgress
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

func (d *scanProgressDisplay) Start() {
	if d == nil {
		return
	}
	go d.run()
}

func (d *scanProgressDisplay) Stop() {
	if d == nil {
		return
	}
	d.stopOnce.Do(func() {
		close(d.stopCh)
		<-d.doneCh
	})
}

func (d *scanProgressDisplay) run() {
	defer close(d.doneCh)

	ticker := time.NewTicker(progressTickInterval)
	defer ticker.Stop()

	frame := 0
	lastWidth := 0
	d.render(frame, &lastWidth)
	frame++

	for {
		select {
		case <-ticker.C:
			d.render(frame, &lastWidth)
			frame++
		case <-d.stopCh:
			if lastWidth > 0 {
				fmt.Fprintf(d.w, "\r%s\r", strings.Repeat(" ", lastWidth))
			}
			return
		}
	}
}

func (d *scanProgressDisplay) render(frame int, lastWidth *int) {
	line := formatScanProgressLine(frame, d.progress.Snapshot())
	padding := ""
	if *lastWidth > len(line) {
		padding = strings.Repeat(" ", *lastWidth-len(line))
	}
	fmt.Fprintf(d.w, "\r%s%s", line, padding)
	*lastWidth = len(line)
}

func formatScanProgressLine(frame int, snapshot inventory.ScanProgressSnapshot) string {
	spinner := progressSpinnerFrames[frame%len(progressSpinnerFrames)]
	phase := "scanning"
	if snapshot.WalkComplete && snapshot.FilesPending > 0 {
		phase = "hashing"
	}

	return fmt.Sprintf(
		"%c [%s] %-8s files: %s done: %s",
		spinner,
		renderProgressTrack(frame, progressTrackWidth),
		phase,
		formatCount(snapshot.FilesDiscovered),
		formatCount(snapshot.FilesProcessed),
	)
}

func renderProgressTrack(frame, width int) string {
	if width <= 0 {
		return ""
	}

	sprite := []byte("..:=#=:..")
	if width <= len(sprite) {
		return string(sprite[:width])
	}

	track := strings.Repeat(" ", width)
	travel := width - len(sprite)
	pos := bouncePosition(frame, travel)

	buf := []byte(track)
	copy(buf[pos:], sprite)
	return string(buf)
}

func bouncePosition(frame, travel int) int {
	if travel <= 0 {
		return 0
	}

	cycle := travel * 2
	step := frame % cycle
	if step > travel {
		return cycle - step
	}
	return step
}

func formatCount(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var b strings.Builder
	prefix := len(s) % 3
	if prefix == 0 {
		prefix = 3
	}
	b.WriteString(s[:prefix])
	for i := prefix; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}
