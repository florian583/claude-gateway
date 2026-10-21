// devin-adapter serves a loopback OpenAI Chat Completions endpoint for SWE-2.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/florian583/claude-gateway/internal/devin"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return errors.New("cannot resolve user home directory")
	}
	listen := flag.String("listen", "127.0.0.1:48105", "literal loopback listen address")
	credentials := flag.String("credentials", filepath.Join(home, ".local", "share", "devin", "credentials.toml"), "Devin CLI credential file (read-only)")
	timeout := flag.Duration("request-timeout", 10*time.Minute, "total request deadline; not a thinking-idle watchdog")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return errors.New("listen must use a literal loopback IP and port")
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	adapter, err := devin.NewHandler(devin.Config{APIKey: os.Getenv("DEVIN_ADAPTER_API_KEY"),
		TokenSource: devin.Credentials(*credentials, func() string { return os.Getenv("WINDSURF_API_KEY") }), Timeout: *timeout,
		Observe: func(e devin.Event) { logger.Info("request", "telemetry", e) }})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return errors.New("cannot bind adapter listener")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Handler: adapter, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	logger.Info("listening", "address", listener.Addr().String(), "model", "swe-2-high")
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return errors.New("adapter server stopped unexpectedly")
}
