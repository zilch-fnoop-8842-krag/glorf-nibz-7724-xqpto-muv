//go:build windows

package main

import (
	"os"
	"os/exec"
	"time"
)

func configureSubprocess(cmd *exec.Cmd) {
	cmd.WaitDelay = 3 * time.Second
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return cmd.Process.Signal(os.Kill)
	}
}
