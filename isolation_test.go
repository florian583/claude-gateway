package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Missing fixtures must fail closed instead of asking the developer's real
// Keychain or starting a real Claude session. Specific credential tests replace
// PATH with their own temporary fake executable when they need positive data.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "claude-proxy-test-tools-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, name := range []string{"security", "claude"} {
		if err = os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nprintf '%s\\n' 'real credential/CLI access disabled in tests' >&2\nexit 90\n"), 0700); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if err = os.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
