package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type clientMigrationFile struct {
	rel           string
	before, after []byte
	existed       bool
	mode          fs.FileMode
	source        string
	sourceInfo    fs.FileInfo
}

// Deliberate allowlist: never import account credentials, telemetry, or arbitrary
// root files. Project directories include Claude's project memory and sessions.
var clientMigrationEntries = []string{
	"CLAUDE.md", "settings.json", "settings.local.json", ".mcp.json",
	"skills", "agents", "commands", "rules", "hooks", "memory", "output-styles", "projects", "plans", "todos", "history.jsonl",
}

func migrateClient(target string, args []string, out io.Writer) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	flags := flag.NewFlagSet("migrate-client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	sourceFlag := flags.String("from", filepath.Join(home, ".claude"), "existing Claude directory")
	apply := flags.Bool("apply", false, "apply migration (default: preview)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: migrate-client [--from PATH] [--apply]")
	}
	if strings.TrimSpace(*sourceFlag) == "" {
		return errors.New("--from must name an existing Claude directory")
	}
	source, err := filepath.Abs(*sourceFlag)
	if err != nil {
		return err
	}
	source, err = filepath.EvalSymlinks(source)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return err
	}
	target = directoryIdentity(target)
	if withinDirectory(source, target) || withinDirectory(target, source) {
		return errors.New("source and client directories must be separate and non-nested")
	}
	if info, err := os.Stat(source); err != nil || !info.IsDir() {
		return errors.New("source must be a directory")
	}
	var plan []clientMigrationFile
	for _, entry := range clientMigrationEntries {
		root := filepath.Join(source, entry)
		if _, err := os.Lstat(root); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			rel, err := filepath.Rel(source, path)
			if err != nil {
				return err
			}
			if d.Type()&os.ModeSymlink != 0 {
				fmt.Fprintf(out, "SKIP symlink %s (review/copy separately)\n", rel)
				return nil
			}
			if d.IsDir() {
				if d.Name() == "node_modules" || d.Name() == ".git" {
					return filepath.SkipDir
				}
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("unsupported source file %s", rel)
			}
			return planClientFile(target, rel, path, info.Mode(), false, &plan, out)
		})
		if err != nil {
			return err
		}
	}
	// Claude's default installation can keep global/project preferences beside
	// ~/.claude rather than inside it. Prefer the profile-local file when present.
	global := filepath.Join(source, ".claude.json")
	if _, err := os.Lstat(global); errors.Is(err, os.ErrNotExist) {
		original, _ := filepath.Abs(*sourceFlag)
		global = original + ".json"
	}
	if info, err := os.Lstat(global); err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("global Claude preferences must be a regular file")
		}
		if err := planClientFile(target, ".claude.json", global, 0600, true, &plan, out); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	mode := "PREVIEW"
	if *apply {
		mode = "APPLY"
	}
	fmt.Fprintf(out, "%s %s -> %s: %d file changes. Existing values win.\n", mode, source, target, len(plan))
	fmt.Fprintln(out, "Credentials and plugin installations are excluded. Reinstall plugins; review copied hooks, MCP commands, and absolute paths before use. Stop Claude sessions using source or destination before --apply.")
	if !*apply || len(plan) == 0 {
		return nil
	}
	if err := os.MkdirAll(target, 0700); err != nil {
		return err
	}
	lockPath := filepath.Join(target, ".gateway-migration.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer os.Remove(lockPath)
	if err := lock.Close(); err != nil {
		return err
	}
	backup, err := os.MkdirTemp(target, ".gateway-migration-backup-")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Backup of replaced JSON files: %s\n", backup)
	for _, item := range plan {
		path := filepath.Join(target, item.rel)
		if err := safeMigrationPath(target, item.rel); err != nil {
			return err
		}
		_, readErr := os.Lstat(path)
		if item.existed {
			current, readErr := os.ReadFile(path)
			if readErr != nil || !bytes.Equal(current, item.before) {
				return fmt.Errorf("destination changed: %s; stop sessions and retry", item.rel)
			}
			backupPath := filepath.Join(backup, item.rel)
			if err := os.MkdirAll(filepath.Dir(backupPath), 0700); err != nil {
				return err
			}
			if err := os.WriteFile(backupPath, item.before, 0600); err != nil {
				return err
			}
		} else if !errors.Is(readErr, os.ErrNotExist) {
			return fmt.Errorf("destination appeared or unreadable: %s", item.rel)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return err
		}
		if !item.existed {
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, item.mode)
			if err != nil {
				return err
			}
			var writeErr error
			if item.source != "" {
				var src *os.File
				src, writeErr = os.Open(item.source)
				if writeErr == nil {
					info, statErr := src.Stat()
					linkInfo, linkErr := os.Lstat(item.source)
					if statErr != nil || linkErr != nil || !linkInfo.Mode().IsRegular() || !os.SameFile(item.sourceInfo, info) || info.Size() != item.sourceInfo.Size() || !info.ModTime().Equal(item.sourceInfo.ModTime()) {
						writeErr = fmt.Errorf("source changed during migration: %s", item.rel)
					} else {
						_, writeErr = io.Copy(f, src)
					}
					writeErr = errors.Join(writeErr, src.Close())
				}
			} else {
				_, writeErr = f.Write(item.after)
			}
			if err := errors.Join(writeErr, f.Close()); err != nil {
				_ = os.Remove(path) // Only this operation's newly created partial file.
				return err
			}
		} else {
			tmp, err := os.CreateTemp(filepath.Dir(path), ".migration-json-")
			if err != nil {
				return err
			}
			_, writeErr := tmp.Write(item.after)
			err = errors.Join(writeErr, tmp.Sync(), tmp.Close())
			if err == nil {
				err = os.Rename(tmp.Name(), path)
			}
			os.Remove(tmp.Name())
			if err != nil {
				return err
			}
		}
	}
	fmt.Fprintln(out, "Migration complete. Source untouched; no login or proxy restart performed.")
	return nil
}

func withinDirectory(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func safeMigrationPath(root, rel string) error {
	path := root
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		path = filepath.Join(path, part)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("destination symlink refused: %s", rel)
		}
	}
	return nil
}

func planClientFile(target, rel, source string, mode fs.FileMode, global bool, plan *[]clientMigrationFile, out io.Writer) error {
	if err := safeMigrationPath(target, rel); err != nil {
		return err
	}
	_, err := os.Lstat(filepath.Join(target, rel))
	existed := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	merge := global || rel == "settings.json" || rel == "settings.local.json" || rel == ".mcp.json"
	if existed && !merge {
		fmt.Fprintf(out, "KEEP %s\n", rel)
		return nil
	}
	if !merge {
		info, err := os.Lstat(source)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("source changed: %s", rel)
		}
		fmt.Fprintf(out, "IMPORT %s\n", rel)
		*plan = append(*plan, clientMigrationFile{rel: rel, source: source, sourceInfo: info, mode: 0600 | mode.Perm()&0111})
		return nil
	}
	var before []byte
	if existed {
		before, err = os.ReadFile(filepath.Join(target, rel))
		if err != nil {
			return err
		}
	}
	after, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if merge {
		incoming, err := migrationJSON(after)
		if err != nil {
			return fmt.Errorf("source %s: %w", rel, err)
		}
		if global {
			selected := map[string]any{}
			for _, key := range []string{"projects", "mcpServers", "theme", "preferredNotifChannel", "editorMode", "verbose", "autoCompactEnabled"} {
				if value, ok := incoming[key]; ok {
					selected[key] = value
				}
			}
			incoming = selected
		} else if rel == "settings.json" || rel == "settings.local.json" {
			// Account auth and routing belong to the gateway launcher, not the old profile.
			for _, key := range []string{"apiKeyHelper", "model", "forceLoginMethod", "forceLoginOrgUUID", "enabledPlugins", "extraKnownMarketplaces"} {
				delete(incoming, key)
			}
			if env, ok := incoming["env"].(map[string]any); ok {
				for key := range env {
					if strings.HasPrefix(key, "ANTHROPIC_") || strings.HasPrefix(key, "CLAUDE_CODE_USE_") || strings.HasPrefix(key, "CLAUDE_CODE_OAUTH_") || key == "CLAUDE_CODE_SUBAGENT_MODEL" || key == "CLAUDE_CONFIG_DIR" || key == "CLAUDE_PROXY_CONFIG" {
						delete(env, key)
					}
				}
			}
		}
		existing := map[string]any{}
		if existed {
			existing, err = migrationJSON(before)
			if err != nil {
				return fmt.Errorf("destination %s: %w", rel, err)
			}
		}
		if !mergeMissingMigrationJSON(existing, incoming) {
			return nil
		}
		after, err = json.MarshalIndent(existing, "", "  ")
		if err != nil {
			return err
		}
		after = append(after, '\n')
	}
	if existed && bytes.Equal(before, after) {
		return nil
	}
	fmt.Fprintf(out, "IMPORT %s\n", rel)
	*plan = append(*plan, clientMigrationFile{rel: rel, before: before, after: after, existed: existed, mode: 0600 | mode.Perm()&0111})
	return nil
}

func migrationJSON(data []byte) (map[string]any, error) {
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if value == nil {
		return nil, errors.New("expected JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("unexpected trailing JSON")
	}
	return value, nil
}

func mergeMissingMigrationJSON(dst, src map[string]any) bool {
	changed := false
	for key, incoming := range src {
		current, exists := dst[key]
		if !exists {
			dst[key] = incoming
			changed = true
			continue
		}
		a, aOK := current.(map[string]any)
		b, bOK := incoming.(map[string]any)
		if aOK && bOK && mergeMissingMigrationJSON(a, b) {
			changed = true
		}
	}
	return changed
}
