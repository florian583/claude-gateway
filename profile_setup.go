package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func addProfile(path string, args []string, out io.Writer) error {
	if len(args) == 0 || !validID(args[0]) || strings.ContainsAny(args[0], "./[]") || strings.HasPrefix(args[0], "-") {
		return errors.New("usage: profiles add NAME [--pool ID] [--config-dir PATH] [--command EXECUTABLE] [--display-name NAME]; profile names use letters, digits, hyphens, or underscores")
	}
	id := args[0]
	flags := flag.NewFlagSet("profiles add", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	pool := flags.String("pool", "", "account pool")
	directory := flags.String("config-dir", filepath.Join("profiles", id), "new profile directory")
	command := flags.String("command", "claude", "Claude executable")
	display := flags.String("display-name", id, "display name")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected profiles add arguments")
	}
	// Serialize config writers. Never remove a lock owned by another process.
	lockPath := path + ".lock"
	lock, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("cannot lock config (check for another writer): %w", err)
	}
	defer os.Remove(lockPath)
	if err := lock.Close(); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("profiles add requires a regular config file; use the target path for symlinks")
	}
	original, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	cfg, err := decodeFileConfig(path, original)
	if err != nil {
		return err
	}
	if _, exists := cfg.Definition.Profiles[id]; exists {
		return fmt.Errorf("profile %q already exists", id)
	}
	if *pool == "" && cfg.ClaudeUsage.Provider != "" {
		*pool = cfg.Definition.Providers[cfg.ClaudeUsage.Provider].Auth.Pool
	}
	if *pool != "" {
		if _, exists := cfg.Definition.AccountPools[*pool]; !exists {
			return fmt.Errorf("unknown account pool %q", *pool)
		}
	}
	profileDir, err := configPath(filepath.Dir(path), *directory)
	if err != nil || profileDir == "" {
		return errors.New("invalid profile directory")
	}
	// Preserve provider-native numeric options exactly while editing two sections.
	var document map[string]any
	decoder := json.NewDecoder(bytes.NewReader(original))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		return err
	}
	profiles, _ := document["profiles"].(map[string]any)
	if profiles == nil {
		profiles = map[string]any{}
		document["profiles"] = profiles
	}
	profiles[id] = map[string]any{"displayName": *display, "command": []string{*command}, "configDir": *directory}
	if *pool != "" {
		pools := document["accountPools"].(map[string]any)
		definition := pools[*pool].(map[string]any)
		definition["profiles"] = append(append([]string(nil), cfg.Definition.AccountPools[*pool].Profiles...), id)
	}
	updated, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	updated = append(updated, '\n')
	if _, err := decodeFileConfig(path, updated); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(profileDir), 0700); err != nil {
		return err
	}
	if err := os.Mkdir(profileDir, 0700); err != nil {
		return fmt.Errorf("profile directory must be new: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// Removes only our newly created directory, and only if still empty.
			_ = os.Remove(profileDir)
		}
	}()
	tmp, err := os.CreateTemp(filepath.Dir(path), ".config-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, writeErr := tmp.Write(updated)
	if err := errors.Join(writeErr, tmp.Sync(), tmp.Close()); err != nil {
		return err
	}
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, original) {
		return errors.New("config changed during profile setup; retry after other edits finish")
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	committed = true
	login := fmt.Sprintf("claude-proxy --config %s profiles login %s", shellQuote(path), shellQuote(id))
	direct := fmt.Sprintf("claude-proxy --config %s profiles open %s --", shellQuote(path), shellQuote(id))
	_, err = fmt.Fprintf(out, "Created profile %s in %s.\nPool: %s\nLogin: %s\nAlias: alias claude-%s=%s\nRestart the proxy to load the updated config.\n", id, profileDir, firstNonEmpty(*pool, "none; add to an account pool before routing"), login, id, shellQuote(direct))
	return err
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
