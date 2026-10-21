package main

import (
	"errors"
	"fmt"
	"os"
	"time"
)

type accountSelectionTimingKey struct{}

// Disk retention is separate from the in-memory rings. Keep at most 32
// segments (~256 MiB at normal flush sizes), expiring closed segments after
// 72 hours. The disk cap can shorten coverage under exceptional traffic.
const (
	metricsArchiveCount = 32
	metricsSegmentBytes = 8 << 20
	metricsRetention    = 72 * time.Hour
)

func rotateMetricsArchives(path string, now time.Time) error {
	for i := metricsArchiveCount; i >= 1; i-- {
		source := fmt.Sprintf("%s.%d", path, i)
		info, err := os.Stat(source)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if i == metricsArchiveCount || now.Sub(info.ModTime()) > metricsRetention {
			if err := os.Remove(source); err != nil {
				return err
			}
			continue
		}
		if err := os.Rename(source, fmt.Sprintf("%s.%d", path, i+1)); err != nil {
			return err
		}
	}
	if err := os.Rename(path, path+".1"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func observedHeaderMS(sample metricsSample) int64 {
	if sample.ResponseHeadersMS > 0 {
		return sample.ResponseHeadersMS
	}
	if sample.Execution != nil && sample.Execution.TransportError == "response_header_timeout" && !sample.Execution.ChainLimited {
		return sample.Execution.HeaderBudgetMS
	}
	return 0
}
