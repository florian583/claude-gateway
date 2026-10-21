// Package devin implements an opt-in adapter for Devin's private Connect protocol.
// It is not a public Devin API client and never starts an agent or executes tools.
package devin

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

type TokenSource func(context.Context) (string, error)

// Credentials reloads the CLI's root TOML key each request. Login/rotation remains
// owned by Devin CLI; no token is cached, copied, logged or refreshed by this service.
// env overrides the file when nonempty. Only single-line TOML strings are supported.
func Credentials(path string, env func() string) TokenSource {
	return func(ctx context.Context) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if env != nil {
			if s := strings.TrimSpace(env()); s != "" {
				return validToken(s)
			}
		}
		f, err := os.Open(path)
		if err != nil {
			return "", errors.New("Devin credential unavailable; authenticate with Devin CLI")
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, 65537))
		if err != nil || len(b) > 65536 {
			return "", errors.New("invalid Devin credential file")
		}
		scan := bufio.NewScanner(strings.NewReader(string(b)))
		var token string
		for scan.Scan() {
			line := strings.TrimSpace(scan.Text())
			if strings.HasPrefix(line, "[") {
				break
			}
			k, v, ok := strings.Cut(line, "=")
			if !ok || strings.TrimSpace(k) != "windsurf_api_key" {
				continue
			}
			if token != "" {
				return "", errors.New("duplicate Devin credential key")
			}
			v = strings.TrimSpace(v)
			if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
				token = v[1 : len(v)-1]
			} else {
				token, err = strconv.Unquote(v)
				if err != nil {
					return "", errors.New("unsupported Devin credential encoding")
				}
			}
		}
		return validToken(token)
	}
}

func validToken(s string) (string, error) {
	if s == "" || len(s) > 16384 || strings.ContainsAny(s, "\r\n\x00") {
		return "", errors.New("invalid or missing Devin credential")
	}
	return s, nil
}
