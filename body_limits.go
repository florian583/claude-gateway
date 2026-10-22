package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type bodyLimitsConfig struct {
	RequestMaxBytes      int64 `json:"requestMaxBytes,omitempty"`
	JSONMaxBytes         int64 `json:"jsonMaxBytes,omitempty"`
	RequestReadTimeoutMS int   `json:"requestReadTimeoutMS,omitempty"`
	JSONReadTimeoutMS    int   `json:"jsonReadTimeoutMS,omitempty"`
	ErrorReadTimeoutMS   int   `json:"errorReadTimeoutMS,omitempty"`
}

const maxErrorBodyBytes int64 = 1 << 20

var errBodyTooLarge = errors.New("body exceeds configured byte limit")
var errBodyReadTimeout = errors.New("body read deadline exceeded")
var errInvalidResponseJSON = errors.New("upstream response is not a valid JSON object")

func (c bodyLimitsConfig) defaults() bodyLimitsConfig {
	if c.RequestMaxBytes == 0 {
		c.RequestMaxBytes = 64 << 20
	}
	if c.JSONMaxBytes == 0 {
		c.JSONMaxBytes = 32 << 20
	}
	if c.RequestReadTimeoutMS == 0 {
		c.RequestReadTimeoutMS = 120000
	}
	if c.JSONReadTimeoutMS == 0 {
		c.JSONReadTimeoutMS = 120000
	}
	if c.ErrorReadTimeoutMS == 0 {
		c.ErrorReadTimeoutMS = 5000
	}
	return c
}
func (c bodyLimitsConfig) validate() error {
	if c.RequestMaxBytes < 0 || c.RequestMaxBytes > 512<<20 || c.JSONMaxBytes < 0 || c.JSONMaxBytes > 512<<20 {
		return errors.New("bodyLimits byte limits must be between 1 and 536870912, or zero for defaults")
	}
	for _, v := range []int{c.RequestReadTimeoutMS, c.JSONReadTimeoutMS, c.ErrorReadTimeoutMS} {
		if v < 0 || v > 600000 {
			return errors.New("bodyLimits timeouts must be between 1 and 600000 milliseconds, or zero for defaults")
		}
	}
	return nil
}

// The caller retains ownership of Close, including any attempt reservation.
func readBoundedResponseBody(ctx context.Context, source io.ReadCloser, limit int64, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		return nil, errBodyReadTimeout
	}
	var timedOut atomic.Bool
	timerDone := make(chan struct{})
	timer := time.AfterFunc(timeout, func() { timedOut.Store(true); _ = source.Close(); close(timerDone) })
	cancelDone := make(chan struct{})
	stopCancel := context.AfterFunc(ctx, func() { _ = source.Close(); close(cancelDone) })
	body, err := io.ReadAll(io.LimitReader(source, limit+1))
	if !timer.Stop() {
		<-timerDone
	}
	if !stopCancel() {
		<-cancelDone
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if timedOut.Load() {
		return nil, errBodyReadTimeout
	}
	if int64(len(body)) > limit {
		return nil, errBodyTooLarge
	}
	return body, err
}

func validResponseJSONObject(body []byte) bool {
	body = bytes.TrimSpace(body)
	return len(body) > 0 && body[0] == '{' && json.Valid(body)
}

// Buffer only before downstream commitment. Streams keep their idle watchdog.
func (s *proxyServer) bufferUpstreamBody(ctx context.Context, resp *http.Response, trace requestTrace, contracts ...responseContract) error {
	isError := resp.StatusCode >= http.StatusBadRequest
	isSSE := strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
	if !isError && isSSE {
		return nil
	}
	limits := s.cfg.BodyLimits.defaults()
	limit := limits.JSONMaxBytes
	timeout := time.Duration(limits.JSONReadTimeoutMS) * time.Millisecond
	if isError {
		limit = maxErrorBodyBytes
		timeout = time.Duration(limits.ErrorReadTimeoutMS) * time.Millisecond
	}
	if !trace.ChainStarted.IsZero() {
		timeout = minDuration(timeout, time.Until(trace.ChainStarted.Add(trace.ChainBudget)))
	}
	body, err := readBoundedResponseBody(ctx, resp.Body, limit, timeout)
	_ = resp.Body.Close()
	if err != nil {
		return err
	}
	if !isError && strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "application/json") && !validResponseJSONObject(body) {
		return errInvalidResponseJSON
	}
	if !isError && len(contracts) > 0 {
		if err := contracts[0].validate(body); err != nil {
			return err
		}
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

type failedBodyReader struct{ err error }

func (r failedBodyReader) Read([]byte) (int, error) { return 0, r.err }

func bodyReadFailure(ctx context.Context, err error) string {
	if ctx.Err() != nil {
		return "client_cancel"
	}
	if errors.Is(err, errBodyReadTimeout) {
		return "response_body_timeout"
	}
	if errors.Is(err, errBodyTooLarge) {
		return "response_body_too_large"
	}
	return "response_body_error"
}

func requestBodyFailure(err error) (int, string) {
	var tooLarge *http.MaxBytesError
	if errors.Is(err, errBodyTooLarge) || errors.As(err, &tooLarge) {
		return http.StatusRequestEntityTooLarge, "request_too_large"
	}
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return http.StatusRequestTimeout, "request_body_timeout"
	}
	return http.StatusBadRequest, "request_body_error"
}

func responseBodyError(provider string, err error) error {
	return fmt.Errorf("provider %s response body failed: %w", provider, err)
}

type responseContract struct {
	openAI     bool
	strict     bool
	toolChoice any
}

func (c responseContract) validate(body []byte) error {
	if c.openAI {
		translated, invalid := translateOpenAIJSONResponseChecked(body, "validation")
		if invalid {
			return errors.New("upstream completion contains no usable response or has malformed tool arguments")
		}
		body = translated
	}
	if !c.strict {
		return nil
	}
	var message struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
			Name string `json:"name"`
		} `json:"content"`
	}
	if json.Unmarshal(body, &message) != nil {
		return errInvalidResponseJSON
	}
	choice, _ := c.toolChoice.(map[string]any)
	requireTool := choice["type"] == "any" || choice["type"] == "tool"
	for _, block := range message.Content {
		if block.Type == "tool_use" && block.Name != "" && (choice["type"] != "tool" || choice["name"] == block.Name) {
			return nil
		}
		if !requireTool && block.Type == "text" && block.Text != "" {
			return nil
		}
	}
	return errors.New("upstream response does not satisfy the requested content contract")
}
