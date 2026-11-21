//go:build !windows

package main

import (
	"os/exec"
	"syscall"
	"time"
)

func configureSubprocess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Keep WaitDelay <= 3s so yt-dlp/ffmpeg teardown + SaveProxies finishes within GitHub Actions' shutdown grace window.
	cmd.WaitDelay = 3 * time.Second
	cmd.Cancel = func() error {
		if cmd.Process == nil || cmd.Process.Pid <= 0 {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
}
