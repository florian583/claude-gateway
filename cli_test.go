package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIConfigArgumentDoesNotChangeProcessState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("CLAUDE_PROXY_CONFIG", "unchanged")
	before := append([]string(nil), os.Args...)
	if err := runCLI(context.Background(), []string{"--config", path, "init"}, nil, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("CLAUDE_PROXY_CONFIG") != "unchanged" || strings.Join(os.Args, "\x00") != strings.Join(before, "\x00") {
		t.Fatal("CLI mutated process arguments or configuration environment")
	}
	if _, err := loadConfigFile(path); err != nil {
		t.Fatal(err)
	}
}

func TestCLIRejectsInvalidCommandsBeforeLoadingConfig(t *testing.T) {
	t.Setenv("CLAUDE_PROXY_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	for _, args := range [][]string{{"--config"}, {"--config", "--help"}, {"unknown"}, {"serve", "extra"}, {"xai-login", "--unexpected"}} {
		if err := runCLI(context.Background(), args, nil, io.Discard, io.Discard); err == nil || strings.Contains(err.Error(), "no such file") {
			t.Fatalf("args=%q error=%v", args, err)
		}
	}
	var help bytes.Buffer
	if err := runCLI(context.Background(), []string{"--help"}, nil, &help, io.Discard); err != nil || !strings.Contains(help.String(), "profiles add") || !strings.Contains(help.String(), "run [-- CLAUDE_ARGS") {
		t.Fatalf("help=%q error=%v", help.String(), err)
	}
}

func TestAccountAndClientLaunchersUseSeparateEnvironments(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "fake-claude")
	script := `#!/bin/sh
printf 'dir=%s\nbase=%s\nauth=%s\nkey=%s\noauth=%s\nheaders=%s\nmodel=%s\nsonnet=%s\nvertex=%s\ncwd=%s\n' "$CLAUDE_CONFIG_DIR" "${ANTHROPIC_BASE_URL-unset}" "${ANTHROPIC_AUTH_TOKEN-unset}" "${ANTHROPIC_API_KEY-unset}" "${CLAUDE_CODE_OAUTH_TOKEN-unset}" "${ANTHROPIC_CUSTOM_HEADERS-unset}" "${ANTHROPIC_MODEL-unset}" "${ANTHROPIC_DEFAULT_SONNET_MODEL-unset}" "${CLAUDE_CODE_USE_VERTEX-unset}" "$PWD"
for arg in "$@"; do printf 'arg=%s\n' "$arg"; done
`
	if err := os.WriteFile(executable, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"CLAUDE_CONFIG_DIR", "ANTHROPIC_BASE_URL", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_CUSTOM_HEADERS", "ANTHROPIC_MODEL", "ANTHROPIC_DEFAULT_SONNET_MODEL", "CLAUDE_CODE_USE_VERTEX"} {
		t.Setenv(key, "inherited-canary")
	}
	f := testClaudeConfig(t)
	f.Listen = "127.0.0.1:48114"
	f.Client = clientDefinition{Command: []string{executable, "fixed"}, ConfigDir: filepath.Join(dir, "client"), Model: "test"}
	f.Aliases["test[1m]"] = "main"
	p := f.Profiles["first"]
	p.Command = []string{executable}
	f.Profiles["first"] = p
	cfg := compileFixture(t, f)
	cwd, _ := os.Getwd()
	cwd = directoryIdentity(cwd)
	var client bytes.Buffer
	if err := runClient(context.Background(), cfg, []string{"--", "--model", "test[1m]", "--print", "hello with spaces"}, nil, &client, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"dir=" + f.Client.ConfigDir, "base=http://127.0.0.1:48114", "auth=claude-proxy", "key=unset", "oauth=unset", "headers=unset", "model=test[1m]", "sonnet=test[1m]", "vertex=unset", "arg=fixed", "arg=hello with spaces", "cwd=" + cwd} {
		if !strings.Contains(client.String(), want+"\n") {
			t.Fatalf("client missing %q: %s", want, client.String())
		}
	}
	client.Reset()
	cfg.Definition.Client.SonnetModel = "test"
	if err := runClient(context.Background(), cfg, []string{"--model=test[1m]"}, nil, &client, io.Discard); err != nil || !strings.Contains(client.String(), "sonnet=test\n") || !strings.Contains(client.String(), "model=test[1m]\n") {
		t.Fatalf("model override=%q error=%v", client.String(), err)
	}
	var direct bytes.Buffer
	if err := runProfilesCommand(context.Background(), cfg, []string{"open", "first", "--", "--print", "direct prompt"}, nil, &direct, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"dir=" + p.ConfigDir, "base=unset", "auth=unset", "key=unset", "oauth=unset", "headers=unset", "model=unset", "sonnet=unset", "vertex=unset", "arg=direct prompt", "cwd=" + cwd} {
		if !strings.Contains(direct.String(), want+"\n") {
			t.Fatalf("account missing %q: %s", want, direct.String())
		}
	}
	var login bytes.Buffer
	if err := runProfilesCommand(context.Background(), cfg, []string{"login", "first"}, nil, &login, io.Discard); err != nil || !strings.Contains(login.String(), "arg=auth\narg=login\narg=--claudeai\n") || !strings.Contains(login.String(), "base=unset\n") {
		t.Fatalf("login=%q error=%v", login.String(), err)
	}
	if err := runProfilesCommand(context.Background(), cfg, []string{"run", "first"}, nil, io.Discard, io.Discard); err == nil {
		t.Fatal("ambiguous profiles run command accepted")
	}
}

func TestClientRejectsMissingOrUnregisteredModel(t *testing.T) {
	f := testConfig("http://127.0.0.1:9999", "anthropic")
	f.Listen = "127.0.0.1:48114"
	cfg := compileFixture(t, f)
	for _, args := range [][]string{nil, {"--model"}, {"--model", "missing"}, {"--model="}, {"auth", "login"}, {"setup-token"}} {
		if err := runClient(context.Background(), cfg, args, nil, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted args %q", args)
		}
	}
	f.Client.SonnetModel = "missing"
	b, _ := json.Marshal(f)
	if _, err := decodeFileConfig(filepath.Join(t.TempDir(), "config.json"), b); err == nil {
		t.Fatal("unregistered model override accepted")
	}
}

func TestClientDirectoryCannotAliasAccountDirectory(t *testing.T) {
	for _, existing := range []bool{false, true} {
		dir := t.TempDir()
		real := filepath.Join(dir, "real")
		alias := filepath.Join(dir, "alias")
		if err := os.Mkdir(real, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(real, alias); err != nil {
			t.Fatal(err)
		}
		if existing {
			if err := os.Mkdir(filepath.Join(real, "profile"), 0700); err != nil {
				t.Fatal(err)
			}
		}
		f := testClaudeConfig(t)
		p := f.Profiles["first"]
		p.ConfigDir = filepath.Join(real, "profile")
		f.Profiles["first"] = p
		f.Client.ConfigDir = filepath.Join(alias, "profile")
		b, _ := json.Marshal(f)
		if _, err := decodeFileConfig(filepath.Join(dir, "config.json"), b); err == nil || !strings.Contains(err.Error(), "client.configDir") {
			t.Fatalf("existing=%t error=%v", existing, err)
		}
	}
}
