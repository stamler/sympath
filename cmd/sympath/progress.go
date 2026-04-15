package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	inventory "sympath"

	"github.com/mattn/go-isatty"
)

const (
	progressTickInterval = 120 * time.Millisecond
	progressLogInterval  = time.Second
	progressTrackWidth   = 20
)

var progressTrackTailFrames = []rune{'▓', '▒', '░'}

type progressOutput struct {
	w           io.Writer
	interactive bool

	mu        sync.Mutex
	liveLine  string
	liveWidth int
	live      bool
}

func newProgressOutput(w io.Writer) *progressOutput {
	if w == nil {
		return nil
	}
	if existing, ok := w.(*progressOutput); ok {
		return existing
	}
	return &progressOutput{
		w:           w,
		interactive: supportsLiveProgress(w),
	}
}

func supportsLiveProgress(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fd := f.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}

func (o *progressOutput) Write(p []byte) (int, error) {
	if o == nil || o.w == nil {
		return len(p), nil
	}
	if !o.interactive {
		return o.w.Write(p)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	hadLive := o.live
	if hadLive {
		o.eraseLiveLineLocked()
	}

	n, err := o.w.Write(p)

	if hadLive {
		o.renderLiveLineLocked(o.liveLine)
	}

	return n, err
}

func (o *progressOutput) setLiveLine(line string) {
	if o == nil || o.w == nil || !o.interactive {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	o.liveLine = line
	o.renderLiveLineLocked(line)
}

func (o *progressOutput) clearLiveLine() {
	if o == nil || o.w == nil || !o.interactive {
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if !o.live {
		return
	}

	o.eraseLiveLineLocked()
	o.live = false
	o.liveLine = ""
	o.liveWidth = 0
}

func (o *progressOutput) renderLiveLineLocked(line string) {
	lineWidth := utf8.RuneCountInString(line)
	padding := ""
	if o.liveWidth > lineWidth {
		padding = strings.Repeat(" ", o.liveWidth-lineWidth)
	}
	fmt.Fprintf(o.w, "\r%s%s", line, padding)
	o.live = true
	o.liveWidth = lineWidth
}

func (o *progressOutput) eraseLiveLineLocked() {
	if o.liveWidth == 0 {
		return
	}
	fmt.Fprintf(o.w, "\r%s\r", strings.Repeat(" ", o.liveWidth))
}

type scanProgressDisplay struct {
	output   *progressOutput
	logger   verboseLogger
	progress *inventory.ScanProgress

	mu      sync.Mutex
	started bool
	stopped bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

func newScanProgressDisplay(logger verboseLogger) (*inventory.ScanProgress, *scanProgressDisplay) {
	progress := &inventory.ScanProgress{}
	display := &scanProgressDisplay{
		output:   logger.progress,
		logger:   logger,
		progress: progress,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	return progress, display
}

func (d *scanProgressDisplay) Start() {
	if d == nil {
		return
	}

	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	d.started = true
	d.mu.Unlock()

	if d.output != nil && d.output.interactive {
		go d.runInteractive()
		return
	}
	go d.runLineMode()
}

func (d *scanProgressDisplay) Stop() {
	if d == nil {
		return
	}

	d.mu.Lock()
	if !d.started || d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	close(d.stopCh)
	d.mu.Unlock()

	<-d.doneCh
}

func (d *scanProgressDisplay) runInteractive() {
	defer close(d.doneCh)

	ticker := time.NewTicker(progressTickInterval)
	defer ticker.Stop()

	frame := 0
	d.output.setLiveLine(formatScanProgressLine(frame, d.progress.Snapshot()))
	frame++

	for {
		select {
		case <-ticker.C:
			d.output.setLiveLine(formatScanProgressLine(frame, d.progress.Snapshot()))
			frame++
		case <-d.stopCh:
			d.output.clearLiveLine()
			return
		}
	}
}

func (d *scanProgressDisplay) runLineMode() {
	defer close(d.doneCh)

	ticker := time.NewTicker(progressLogInterval)
	defer ticker.Stop()

	lastText := d.logSnapshot("")

	for {
		select {
		case <-ticker.C:
			lastText = d.logSnapshot(lastText)
		case <-d.stopCh:
			d.logSnapshot(lastText)
			return
		}
	}
}

func (d *scanProgressDisplay) logSnapshot(lastText string) string {
	text := formatScanProgressText(d.progress.Snapshot())
	if text == lastText || !d.logger.Enabled(logLevelInfo) {
		return lastText
	}
	d.logger.Infof("%s", text)
	return text
}

type itemProgressDisplay struct {
	output *progressOutput
	logger verboseLogger
	phase  string

	mu      sync.Mutex
	current string
	started bool
	stopped bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

func newItemProgressDisplay(logger verboseLogger, phase string) *itemProgressDisplay {
	return &itemProgressDisplay{
		output: logger.progress,
		logger: logger,
		phase:  phase,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

func (d *itemProgressDisplay) Advance(liveText, lineText string) {
	if d == nil {
		return
	}

	if d.output != nil && d.output.interactive {
		d.mu.Lock()
		d.current = liveText
		if !d.started {
			d.started = true
			go d.run()
		}
		d.mu.Unlock()
		return
	}

	if lineText != "" && d.logger.Enabled(logLevelInfo) {
		d.logger.Infof("%s", lineText)
	}
}

func (d *itemProgressDisplay) Stop() {
	if d == nil {
		return
	}

	d.mu.Lock()
	if !d.started || d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	close(d.stopCh)
	d.mu.Unlock()

	<-d.doneCh
}

func (d *itemProgressDisplay) run() {
	defer close(d.doneCh)

	ticker := time.NewTicker(progressTickInterval)
	defer ticker.Stop()

	frame := 0
	d.render(frame)
	frame++

	for {
		select {
		case <-ticker.C:
			d.render(frame)
			frame++
		case <-d.stopCh:
			d.output.clearLiveLine()
			return
		}
	}
}

func (d *itemProgressDisplay) render(frame int) {
	d.mu.Lock()
	current := d.current
	d.mu.Unlock()

	if current == "" {
		return
	}

	d.output.setLiveLine(formatItemProgressLine(frame, d.phase, current))
}

func formatScanProgressLine(frame int, snapshot inventory.ScanProgressSnapshot) string {
	return fmt.Sprintf("[%s] %s", renderProgressTrack(frame, progressTrackWidth), formatScanProgressText(snapshot))
}

func formatScanProgressText(snapshot inventory.ScanProgressSnapshot) string {
	return fmt.Sprintf(
		"Scanning files: %s done: %s",
		formatCount(snapshot.FilesDiscovered),
		formatCount(snapshot.FilesProcessed),
	)
}

func formatItemProgressLine(frame int, phase, item string) string {
	return fmt.Sprintf("[%s] %s: %s", renderProgressTrack(frame, progressTrackWidth), phase, item)
}

func renderProgressTrack(frame, width int) string {
	if width <= 0 {
		return ""
	}

	track := make([]rune, width)
	for i := range track {
		track[i] = ' '
	}

	headPos, movingRight := bounceState(frame, width-1)
	track[headPos] = '█'

	for offset, shade := range progressTrackTailFrames {
		if movingRight {
			tailPos := headPos - 1 - offset
			if tailPos < 0 {
				break
			}
			track[tailPos] = shade
			continue
		}

		tailPos := headPos + 1 + offset
		if tailPos >= width {
			break
		}
		track[tailPos] = shade
	}

	return string(track)
}

func bounceState(frame, travel int) (int, bool) {
	if travel <= 0 {
		return 0, true
	}

	cycle := travel * 2
	step := frame % cycle
	if step > travel {
		return cycle - step, false
	}
	return step, true
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
