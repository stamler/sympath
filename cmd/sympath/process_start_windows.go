//go:build windows

package main

import (
	"os/exec"
	"syscall"
)

const detachedProcess = 0x00000008

func startDetachedProcess(name string, args []string) error {
	cmd := exec.Command(name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | detachedProcess,
	}
	return cmd.Start()
}
