package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
)

func stripArgumentSeparator(args []string) []string {
	if len(args) > 0 && args[0] == "--" {
		return args[1:]
	}
	return args
}

// Resolve existing ancestors too: the final directory may not exist yet.
func directoryIdentity(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	parent := filepath.Dir(path)
	if parent == path {
		return path
	}
	return filepath.Join(directoryIdentity(parent), filepath.Base(path))
}

func sameDirectory(a, b string) bool {
	aInfo, aErr := os.Stat(a)
	bInfo, bErr := os.Stat(b)
	if aErr == nil && bErr == nil {
		return os.SameFile(aInfo, bInfo)
	}
	return directoryIdentity(a) == directoryIdentity(b)
}

func runClient(ctx context.Context, cfg config, args []string, in io.Reader, out, errOut io.Writer) error {
	client := cfg.Definition.Client
	args = stripArgumentSeparator(args)
	if len(args) > 0 && (args[0] == "auth" || args[0] == "setup-token") {
		return errors.New("manage subscription credentials with profiles login/open, not the proxy client")
	}
	model := client.Model
	for i := 0; i < len(args); i++ {
		if args[i] == "--model" {
			i++
			if i == len(args) {
				return errors.New("--model requires a configured alias")
			}
			model = args[i]
		} else if value, ok := strings.CutPrefix(args[i], "--model="); ok {
			model = value
		}
	}
	if model == "" || cfg.Definition.Aliases[model] == "" {
		return errors.New("set client.model or pass --model with a configured alias")
	}
	_, port, err := net.SplitHostPort(cfg.Listen)
	if err != nil || port == "0" {
		return errors.New("run requires a fixed proxy listen port")
	}
	for id, profile := range cfg.Definition.Profiles {
		if profile.ConfigDir != "" && sameDirectory(profile.ConfigDir, client.ConfigDir) {
			return fmt.Errorf("client.configDir must differ from profile %q configDir", id)
		}
	}
	if err := os.MkdirAll(client.ConfigDir, 0700); err != nil {
		return err
	}
	p := profileDefinition{ConfigDir: client.ConfigDir}
	cmd, err := makeProfileCommand(ctx, p, client.Command, args)
	if err != nil {
		return err
	}
	// This token only satisfies Claude Code's gateway authentication check.
	// The proxy strips it and supplies the selected provider's credentials.
	cmd.Env = append(cmd.Env,
		"ANTHROPIC_BASE_URL=http://"+cfg.Listen,
		"ANTHROPIC_AUTH_TOKEN=claude-proxy",
		"ANTHROPIC_MODEL="+model,
		"ANTHROPIC_DEFAULT_OPUS_MODEL="+firstNonEmpty(client.OpusModel, model),
		"ANTHROPIC_DEFAULT_SONNET_MODEL="+firstNonEmpty(client.SonnetModel, model),
		"ANTHROPIC_DEFAULT_HAIKU_MODEL="+firstNonEmpty(client.HaikuModel, model),
	)
	cmd.Dir = "" // Inherit the caller's project directory.
	return runAttachedCommand(cmd, in, out, errOut)
}
