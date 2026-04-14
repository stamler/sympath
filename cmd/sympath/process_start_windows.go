//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

func startDetachedProcess(name string, args []string) error {
	cmd := exec.Command(name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | syscall.DETACHED_PROCESS,
	}
	return cmd.Start()
}
