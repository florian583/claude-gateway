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

func setupTestConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := createConfig(path, io.Discard); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAddProfileCreatesIsolatedDirectoryAndPoolMembership(t *testing.T) {
	path := setupTestConfig(t)
	var out bytes.Buffer
	if err := runCLI(context.Background(), []string{"--config", path, "profiles", "add", "backup", "--display-name", "Backup"}, nil, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Definition.Profiles["backup"]
	wantDir := filepath.Join(filepath.Dir(path), "profiles", "backup")
	if p.DisplayName != "Backup" || p.ConfigDir != wantDir || p.CredentialsService == "" || strings.Join(p.Command, " ") != "claude" {
		t.Fatalf("unexpected profile: %#v", p)
	}
	if got := cfg.Definition.AccountPools["claude"].Profiles; strings.Join(got, ",") != "work,backup" {
		t.Fatalf("pool: %v", got)
	}
	for file, mode := range map[string]os.FileMode{path: 0600, wantDir: 0700} {
		info, err := os.Stat(file)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatalf("incorrect permissions for %s", file)
		}
	}
	entries, _ := os.ReadDir(wantDir)
	if len(entries) != 0 {
		t.Fatal("profile setup created credentials or launched Claude")
	}
	if !strings.Contains(out.String(), "profiles login") || !strings.Contains(out.String(), "Alias: alias claude-backup=") {
		t.Fatalf("missing next steps: %s", out.String())
	}
}

func TestAddProfileRefusesOverwritesAndForeignLocks(t *testing.T) {
	for _, scenario := range []string{"existing-profile", "existing-directory", "locked", "unknown-pool", "traversal", "client-directory", "empty-command"} {
		t.Run(scenario, func(t *testing.T) {
			path := setupTestConfig(t)
			args := []string{"backup"}
			var protected string
			switch scenario {
			case "existing-profile":
				args = []string{"work"}
			case "existing-directory":
				protected = filepath.Join(filepath.Dir(path), "profiles", "backup")
				if err := os.MkdirAll(protected, 0700); err != nil {
					t.Fatal(err)
				}
			case "locked":
				protected = path + ".lock"
				if err := os.WriteFile(protected, []byte("another writer"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unknown-pool":
				args = append(args, "--pool", "missing")
			case "traversal":
				args = []string{"../backup"}
			case "client-directory":
				args = append(args, "--config-dir", "./client")
			case "empty-command":
				args = append(args, "--command", "")
			}
			before, _ := os.ReadFile(path)
			if err := addProfile(path, args, io.Discard); err == nil {
				t.Fatal("invalid setup succeeded")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("failed setup changed config")
			}
			if protected != "" {
				if _, err := os.Stat(protected); err != nil {
					t.Fatal("setup removed pre-existing state")
				}
			}
		})
	}
}

func TestAddProfilePreservesNumericOptions(t *testing.T) {
	path := setupTestConfig(t)
	var doc map[string]any
	if err := json.Unmarshal([]byte(starterConfig), &doc); err != nil {
		t.Fatal(err)
	}
	providers := doc["providers"].(map[string]any)
	providers["claude"].(map[string]any)["requestOverrides"] = map[string]any{"seed": json.Number("9007199254740993")}
	before, _ := json.Marshal(doc)
	if err := os.WriteFile(path, before, 0600); err != nil {
		t.Fatal(err)
	}
	if err := addProfile(path, []string{"backup"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Contains(after, []byte("9007199254740993")) {
		t.Fatal("profile setup rounded an existing provider option")
	}
}
