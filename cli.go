package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const starterConfig = `{
  "version": 1,
  "listen": "127.0.0.1:48104",
  "stateDir": "./state",
  "client": {"command": ["claude"], "configDir": "./client", "model": "worker"},
  "profiles": {
    "work": {"displayName": "Work", "command": ["claude"], "configDir": "./profiles/work"}
  },
  "accountPools": {"claude": {"profiles": ["work"]}},
  "providers": {
    "claude": {
      "protocol": "anthropic", "variant": "claude-subscription",
      "baseURL": "https://api.anthropic.com", "billing": "subscription",
      "auth": {"type": "claude-profile-pool", "pool": "claude"}
    }
  },
  "models": {"worker": {"provider": "claude", "upstream": "REPLACE_WITH_YOUR_MODEL_ID", "supportsTools": true}},
  "chains": {"worker": {"steps": [{"model": "worker"}]}},
  "aliases": {"worker": "worker"}
}
`

const cliHelp = `Usage: claude-proxy [--config PATH] COMMAND

  init                       Create a config file
  validate-config            Check configuration without contacting providers
  serve                      Start the proxy (default)
  run [-- CLAUDE_ARGS...]     Open Claude Code through the proxy
  profiles add NAME [OPTIONS] Create a profile and add it to an account pool
  profiles login ID          Log in a subscription account directly
  profiles open ID [-- ARGS]  Open that account directly, without the proxy
  profiles status [ID]       Check account login status
  profiles doctor [ID]       Check profile setup and credential storage
  xai-login [--no-browser]    Authorize an xAI subscription
  xai-status                 Check xAI authorization
  --version                  Print build information
`

func runCLI(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer) error {
	var path string
	if len(args) > 0 && args[0] == "--config" {
		if len(args) < 2 || strings.TrimSpace(args[1]) == "" || strings.HasPrefix(args[1], "--") {
			return errors.New("--config requires a path before the command")
		}
		var err error
		path, err = filepath.Abs(args[1])
		if err != nil {
			return err
		}
		args = args[2:]
	}
	command := "serve"
	if len(args) > 0 {
		command, args = args[0], args[1:]
	}
	switch command {
	case "help", "--help", "-h":
		_, err := io.WriteString(out, cliHelp)
		return err
	case "version", "--version":
		_, err := fmt.Fprintf(out, "claude-proxy config-v1 source=%s\n", buildSourceHash)
		return err
	case "init", "validate-config", "serve", "xai-status":
		if len(args) != 0 {
			return fmt.Errorf("%s does not accept arguments", command)
		}
	case "xai-login":
		if len(args) > 1 || len(args) == 1 && args[0] != "--no-browser" {
			return errors.New("usage: xai-login [--no-browser]")
		}
	case "profiles", "run":
	default:
		return fmt.Errorf("unknown command %q (use --help)", command)
	}
	if path == "" {
		var err error
		path, err = configFilePath()
		if err != nil {
			return err
		}
	}
	if command == "init" {
		return createConfig(path, out)
	}
	if command == "profiles" && len(args) > 0 && args[0] == "add" {
		return addProfile(path, args[1:], out)
	}
	cfg, err := loadConfigFile(path)
	if err != nil {
		return err
	}
	switch command {
	case "serve":
		return serve(cfg)
	case "validate-config":
		f := cfg.Definition
		_, err = fmt.Fprintf(out, "config valid providers=%d models=%d accountPools=%d modelPools=%d chains=%d aliases=%d\n", len(f.Providers), len(f.Models), len(f.AccountPools), len(f.ModelPools), len(f.Chains), len(f.Aliases))
		return err
	case "profiles":
		return runProfilesCommand(ctx, cfg, args, in, out, errOut)
	case "run":
		return runClient(ctx, cfg, args, in, out, errOut)
	case "xai-login":
		return runXAIOAuthLogin(ctx, cfg, out, len(args) == 0)
	case "xai-status":
		return printXAIOAuthStatus(cfg, out)
	}
	return nil
}

func createConfig(path string, out io.Writer) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := io.WriteString(f, starterConfig)
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "Created %s. Set the upstream model ID before starting.\n", path)
	return err
}
