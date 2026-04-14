//go:build !windows

package main

import "os/exec"

func startDetachedProcess(name string, args []string) error {
	return exec.Command(name, args...).Start()
}
