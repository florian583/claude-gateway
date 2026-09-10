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

func migrationWrite(t *testing.T, root, rel, data string) {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestClientMigrationPreviewApplyAndIdempotence(t *testing.T) {
	root := t.TempDir()
	source, target := filepath.Join(root, "old"), filepath.Join(root, "client")
	migrationWrite(t, source, "CLAUDE.md", "my preferences")
	migrationWrite(t, source, "projects/project/memory/MEMORY.md", "my memory")
	migrationWrite(t, source, "skills/example/SKILL.md", "example")
	migrationWrite(t, source, "settings.json", `{"model":"old-model","apiKeyHelper":"old-helper","enabledPlugins":{"old":true},"env":{"ANTHROPIC_BASE_URL":"old-url","MCP_EXAMPLE":"example"},"permissions":{"allow":["Read"]},"theme":"dark"}`)
	migrationWrite(t, source, ".credentials.json", "never import")
	migrationWrite(t, source, "plugins/installed_plugins.json", "never import")
	migrationWrite(t, source, ".claude.json", `{"oauthAccount":{"example":true},"mcpServers":{"example":{"command":"example"}},"projects":{"/example/project":{"allowedTools":["Read"]}}}`)
	var out bytes.Buffer
	if err := migrateClient(target, []string{"--from", source}, &out); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatal("preview wrote destination")
	}
	migrationWrite(t, target, "settings.json", `{"model":"gateway-model","env":{"ANTHROPIC_BASE_URL":"gateway-url"},"theme":"light"}`)
	migrationWrite(t, target, "CLAUDE.md", "keep destination")
	if err := migrateClient(target, []string{"--from", source, "--apply"}, &out); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"projects/project/memory/MEMORY.md", "skills/example/SKILL.md"} {
		if _, err := os.Stat(filepath.Join(target, file)); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := os.ReadFile(filepath.Join(target, "settings.json"))
	var settings map[string]any
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatal(err)
	}
	if settings["model"] != "gateway-model" || settings["theme"] != "light" || settings["apiKeyHelper"] != nil || settings["enabledPlugins"] != nil {
		t.Fatalf("unsafe settings merge: %s", data)
	}
	env := settings["env"].(map[string]any)
	if env["ANTHROPIC_BASE_URL"] != "gateway-url" || env["MCP_EXAMPLE"] != "example" {
		t.Fatal("routing changed or MCP env missing")
	}
	global, _ := os.ReadFile(filepath.Join(target, ".claude.json"))
	if bytes.Contains(global, []byte("oauthAccount")) || !bytes.Contains(global, []byte("mcpServers")) {
		t.Fatal("incorrect global preference selection")
	}
	for _, file := range []string{".credentials.json", "plugins"} {
		if _, err := os.Stat(filepath.Join(target, file)); !os.IsNotExist(err) {
			t.Fatal("excluded state imported")
		}
	}
	kept, _ := os.ReadFile(filepath.Join(target, "CLAUDE.md"))
	if string(kept) != "keep destination" {
		t.Fatal("existing file overwritten")
	}
	backups, _ := filepath.Glob(filepath.Join(target, ".gateway-migration-backup-*", "settings.json"))
	if len(backups) != 1 {
		t.Fatal("missing pre-merge backup")
	}
	if err := migrateClient(target, []string{"--from", source, "--apply"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	again, _ := os.ReadFile(filepath.Join(target, "settings.json"))
	if !bytes.Equal(data, again) {
		t.Fatal("rerun changed settings")
	}
}

func TestClientMigrationRejectsUnsafeDestinationsAndInvalidJSON(t *testing.T) {
	for _, scenario := range []string{"same", "nested", "reverse-nested", "destination-symlink", "invalid-json", "trailing-json", "foreign-lock"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			source, target := filepath.Join(root, "old"), filepath.Join(root, "client")
			migrationWrite(t, source, "skills/example/SKILL.md", "example")
			switch scenario {
			case "same":
				target = source
			case "nested":
				target = filepath.Join(source, "client")
			case "reverse-nested":
				target = root
			case "destination-symlink":
				if err := os.MkdirAll(target, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(source, filepath.Join(target, "skills")); err != nil {
					t.Fatal(err)
				}
			case "invalid-json":
				migrationWrite(t, source, "settings.json", "{")
			case "trailing-json":
				migrationWrite(t, source, "settings.json", "{} {}")
			case "foreign-lock":
				migrationWrite(t, target, ".gateway-migration.lock", "other writer")
			}
			if err := migrateClient(target, []string{"--from", source, "--apply"}, io.Discard); err == nil {
				t.Fatal("unsafe migration accepted")
			}
			if scenario == "foreign-lock" {
				data, _ := os.ReadFile(filepath.Join(target, ".gateway-migration.lock"))
				if string(data) != "other writer" {
					t.Fatal("foreign lock changed")
				}
			}
		})
	}
}

func TestClientMigrationSiblingGlobalAndSourceSymlinks(t *testing.T) {
	root := t.TempDir()
	source, target := filepath.Join(root, ".claude"), filepath.Join(root, "client")
	migrationWrite(t, source, "CLAUDE.md", "instructions")
	migrationWrite(t, root, ".claude.json", `{"theme":"dark","oauthAccount":{"example":true}}`)
	if err := os.Symlink(filepath.Join(source, "CLAUDE.md"), filepath.Join(source, "hooks")); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := migrateClient(target, []string{"--from", source, "--apply"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "SKIP symlink hooks") {
		t.Fatal("missing symlink report")
	}
	data, _ := os.ReadFile(filepath.Join(target, ".claude.json"))
	if !bytes.Contains(data, []byte("dark")) || bytes.Contains(data, []byte("oauthAccount")) {
		t.Fatal("sibling global config not sanitized")
	}
}

func TestClientMigrationCLIUsesConfiguredDestination(t *testing.T) {
	configPath := setupTestConfig(t)
	source := filepath.Join(t.TempDir(), "legacy")
	migrationWrite(t, source, "memory/MEMORY.md", "remember this")
	if err := runCLI(context.Background(), []string{"--config", configPath, "migrate-client", "--from", source, "--apply"}, nil, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfigFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(cfg.Definition.Client.ConfigDir, "memory", "MEMORY.md"))
	if err != nil || string(data) != "remember this" {
		t.Fatal("CLI did not use client.configDir")
	}
}

func TestClientMigrationStripsOAuthAndRoutingEnvironment(t *testing.T) {
	root := t.TempDir()
	source, target := filepath.Join(root, "old"), filepath.Join(root, "client")
	migrationWrite(t, source, "settings.json", `{"env":{"CLAUDE_CODE_OAUTH_TOKEN":"synthetic-example","CLAUDE_CODE_SUBAGENT_MODEL":"old","CLAUDE_PROXY_CONFIG":"old-config","CLAUDE_CODE_USE_VERTEX":"1","MCP_EXAMPLE":"keep"}}`)
	if err := migrateClient(target, []string{"--from", source, "--apply"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(target, "settings.json"))
	doc, err := migrationJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	env := doc["env"].(map[string]any)
	if len(env) != 1 || env["MCP_EXAMPLE"] != "keep" {
		t.Fatal("old account/routing env imported")
	}
}
