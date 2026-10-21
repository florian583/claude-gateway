package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type usageSources []string

func (s *usageSources) String() string         { return fmt.Sprint([]string(*s)) }
func (s *usageSources) Set(value string) error { *s = append(*s, value); return nil }

// Offline, explicit files only. Never opens config, credentials or a listener.
// Rebuild in a fresh directory; operators compare before activating anything.
func rebuildUsageCLI(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("rebuild-usage", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var sources usageSources
	var destination string
	flags.Var(&sources, "source", "Metrics JSONL file (repeatable)")
	flags.StringVar(&destination, "out", "", "New output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(sources) == 0 || destination == "" || len(flags.Args()) != 0 {
		return errors.New("usage: rebuild-usage --source FILE [--source FILE] --out NEW_DIRECTORY")
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		return errors.New("rebuild output must not exist")
	}
	work, err := os.MkdirTemp(filepath.Dir(destination), ".usage-rebuild-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	store := newRollupStore(work)
	defer store.close()
	seen := map[string]bool{}
	records, duplicates, legacy := 0, 0, 0
	for _, path := range sources {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 64<<10), 4<<20)
		for scanner.Scan() {
			var sample metricsSample
			if err := json.Unmarshal(scanner.Bytes(), &sample); err != nil {
				file.Close()
				return fmt.Errorf("malformed canonical event in %s", path)
			}
			if _, err := time.Parse(time.RFC3339Nano, sample.Timestamp); err != nil {
				file.Close()
				return fmt.Errorf("invalid event timestamp in %s", path)
			}
			if sample.EventID != "" {
				if seen[sample.EventID] {
					duplicates++
					continue
				}
				seen[sample.EventID] = true
			} else {
				legacy++
			}
			store.observe(sample)
			records++
		}
		scanErr, closeErr := scanner.Err(), file.Close()
		if scanErr != nil {
			return scanErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	// close cancels timers and flushes without retention sweeping historical data.
	store.close()
	if err := store.persistenceStatus(); err != "" {
		return fmt.Errorf("rebuild persistence: %s", err)
	}
	if err := os.Rename(work, destination); err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(map[string]any{"records": records, "deduplicated": duplicates, "legacyWithoutIdentity": legacy,
		"output": destination, "coverage": "Only supplied persisted events; unflushed crash gaps and missing historical sources remain unknown. No native/gateway totals are added together."})
}
