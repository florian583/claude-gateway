package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

func profileEnvironment(p profileDefinition) []string {
	// Account management must not inherit consumer routing or credentials.
	deny := map[string]bool{"CLAUDE_CONFIG_DIR": true, "CLAUDE_CODE_OAUTH_TOKEN": true, "ANTHROPIC_BASE_URL": true, "ANTHROPIC_AUTH_TOKEN": true, "ANTHROPIC_API_KEY": true, "ANTHROPIC_CUSTOM_HEADERS": true, "ANTHROPIC_MODEL": true, "CLAUDE_CODE_SUBAGENT_MODEL": true, "CLAUDE_CODE_USE_BEDROCK": true, "CLAUDE_CODE_USE_VERTEX": true, "CLAUDE_CODE_USE_FOUNDRY": true, "CLAUDE_PROXY_CONFIG": true, "ANTHROPIC_PROXY_CONFIG": true}
	env := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !deny[key] && !strings.HasPrefix(key, "ANTHROPIC_DEFAULT_") && !strings.HasPrefix(key, "ANTHROPIC_CUSTOM_MODEL_") {
			env = append(env, entry)
		}
	}
	if p.ConfigDir != "" && !p.DefaultDirectory {
		env = append(env, "CLAUDE_CONFIG_DIR="+p.ConfigDir)
	}
	return env
}
func makeProfileCommand(ctx context.Context, p profileDefinition, argv, extra []string) (*exec.Cmd, error) {
	if e := checkArgv(argv); e != nil {
		return nil, e
	}
	binary, e := exec.LookPath(argv[0])
	if e != nil {
		return nil, fmt.Errorf("profile executable unavailable: %s (shell aliases are not executable)", argv[0])
	}
	cmd := exec.CommandContext(ctx, binary, append(append([]string(nil), argv[1:]...), extra...)...)
	cmd.Env = profileEnvironment(p)
	cmd.Dir = os.TempDir()
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd, nil
}
func runAttachedCommand(cmd *exec.Cmd, in io.Reader, out, errOut io.Writer) error {
	cmd.Stdin, cmd.Stdout, cmd.Stderr = in, out, errOut
	return cmd.Run()
}

func runProfilesCommand(ctx context.Context, cfg config, args []string, in io.Reader, out, errOut io.Writer) error {
	if cfg.Definition == nil || len(args) == 0 {
		return errors.New("usage: profiles login|status|doctor|open [ID] [-- ARGS...]")
	}
	action := args[0]
	if action != "login" && action != "status" && action != "doctor" && action != "open" {
		return errors.New("unknown profiles action; use open for a direct account or top-level run for the proxy")
	}
	ids := sortedKeys(cfg.Definition.Profiles)
	extra := []string{}
	if len(args) > 1 {
		id := args[1]
		if _, ok := cfg.Definition.Profiles[id]; !ok {
			return fmt.Errorf("unknown profile %q", id)
		}
		ids = []string{id}
		extra = args[2:]
	}
	if (action == "login" || action == "open") && len(args) < 2 {
		return errors.New("login/open require one explicit profile ID")
	}
	if action != "open" && len(extra) > 0 {
		return errors.New("unexpected profile arguments")
	}
	if len(extra) > 0 && extra[0] == "--" {
		extra = extra[1:]
	}
	var failures []string
	for _, id := range ids {
		p := cfg.Definition.Profiles[id]
		if action == "doctor" {
			_, exeErr := exec.LookPath(p.Command[0])
			info, dirErr := os.Stat(p.ConfigDir)
			report := map[string]any{"profile": id, "displayName": p.DisplayName, "executableAvailable": exeErr == nil, "configDirectoryExists": dirErr == nil && info.IsDir(), "credentialBackend": "macOS Keychain", "platformSupported": runtime.GOOS == "darwin", "credentialService": p.CredentialsService, "refreshConfigured": len(p.RefreshCommand) > 0}
			if p.ConfigDir == "" {
				report["configDirectoryExists"] = nil
			}
			// Presence only: never request the password (-w) for doctor.
			if runtime.GOOS == "darwin" {
				checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				cmd := exec.CommandContext(checkCtx, "security", "find-generic-password", "-s", p.CredentialsService)
				report["credentialPresent"] = cmd.Run() == nil
				cancel()
			}
			_ = json.NewEncoder(out).Encode(report)
			if exeErr != nil || (p.ConfigDir != "" && (dirErr != nil || !info.IsDir())) || runtime.GOOS != "darwin" || report["credentialPresent"] != true {
				failures = append(failures, id)
			}
			continue
		}
		if (action == "login" || action == "open") && p.ConfigDir != "" {
			if e := os.MkdirAll(p.ConfigDir, 0700); e != nil {
				return e
			}
		}
		callCtx := ctx
		cancel := func() {}
		if action == "status" {
			callCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
		}
		argv := []string{"auth", "status", "--json"}
		if action == "login" {
			argv = []string{"auth", "login", "--claudeai"}
		}
		if action == "open" {
			argv = extra
		}
		cmd, e := makeProfileCommand(callCtx, p, p.Command, argv)
		if e != nil {
			cancel()
			return e
		}
		if action == "login" || action == "open" {
			if action == "open" {
				cmd.Dir = "" // Inherit the caller's project directory.
			}
			e = runAttachedCommand(cmd, in, out, errOut)
			cancel()
			return e
		}
		var result limitedBuffer
		result.limit = 64 << 10
		cmd.Stdout = &result
		e = cmd.Run()
		cancel()
		var status struct {
			LoggedIn bool `json:"loggedIn"`
		}
		parseErr := json.Unmarshal(result.Bytes(), &status)
		ok := e == nil && parseErr == nil && status.LoggedIn
		// Whitelisted fields only: wrappers cannot leak secrets through raw output.
		_ = json.NewEncoder(out).Encode(map[string]any{"profile": id, "displayName": p.DisplayName, "loggedIn": ok, "refreshConfigured": len(p.RefreshCommand) > 0})
		if !ok {
			failures = append(failures, id)
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("profile checks failed: %s", strings.Join(failures, ", "))
	}
	return nil
}

type limitedBuffer struct {
	buffer bytes.Buffer
	limit  int
}

func (b *limitedBuffer) Bytes() []byte { return b.buffer.Bytes() }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.buffer.Len()+len(p) > b.limit {
		return 0, errors.New("command output exceeds limit")
	}
	return b.buffer.Write(p)
}
