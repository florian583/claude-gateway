package devin

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const endpoint = "https://server.codeium.com/exa.api_server_pb.ApiServerService/GetChatMessage"

type Usage struct {
	Prompt     int64 `json:"prompt_tokens"`
	Completion int64 `json:"completion_tokens"`
	Total      int64 `json:"total_tokens"`
}

// Event contains only non-sensitive operational metadata. No prompts, credentials,
// tool arguments, upstream free-text errors, or account identifiers are retained.
type Event struct {
	RequestID    string `json:"request_id"`
	Model        string `json:"model"`
	ElapsedMS    int64  `json:"elapsed_ms"`
	FirstDeltaMS int64  `json:"first_delta_ms,omitempty"`
	Chunks       int    `json:"chunks"`
	Outcome      string `json:"outcome"`
	Usage        *Usage `json:"usage,omitempty"`
}
type Config struct {
	APIKey      string
	TokenSource TokenSource
	HTTPClient  *http.Client
	Timeout     time.Duration
	Observe     func(Event)
}
type Handler struct {
	cfg    Config
	client http.Client
}

func NewHandler(c Config) (*Handler, error) {
	if len(c.APIKey) < 16 || strings.ContainsAny(c.APIKey, "\r\n") {
		return nil, errors.New("adapter API key must contain at least 16 characters")
	}
	if c.TokenSource == nil {
		return nil, errors.New("credential source required")
	}
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Minute
	}
	if c.Timeout < time.Second {
		return nil, errors.New("timeout must be at least one second")
	}
	client := http.Client{}
	if c.HTTPClient != nil {
		client = *c.HTTPClient
	} else {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		// The request context owns the deadline; no separate short thinking watchdog.
		client.Transport = transport
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Handler{c, client}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		w.WriteHeader(200)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+h.cfg.APIKey)) != 1 {
		writeError(w, &apiError{401, "invalid_api_key", "adapter authentication required"}, false)
		return
	}
	if r.URL.Path == "/v1/models" && r.Method == http.MethodGet {
		writeJSON(w, 200, map[string]any{"object": "list", "data": []any{map[string]any{"id": "swe-2-high", "object": "model", "owned_by": "devin"}}})
		return
	}
	if r.URL.Path != "/v1/chat/completions" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		w.WriteHeader(405)
		return
	}
	var req Request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, &apiError{400, "invalid_request_error", "invalid, unsupported or oversized request JSON"}, false)
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		writeError(w, &apiError{400, "invalid_request_error", "trailing request data"}, false)
		return
	}
	if err := req.validate(); err != nil {
		writeError(w, &apiError{400, "invalid_request_error", err.Error()}, false)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.Timeout)
	defer cancel()
	id := "chatcmpl-" + nonce(16)
	start := time.Now()
	event := Event{RequestID: id, Model: req.Model, Outcome: "error"}
	defer func() {
		event.ElapsedMS = time.Since(start).Milliseconds()
		if h.cfg.Observe != nil {
			h.cfg.Observe(event)
		}
	}()
	token, err := h.cfg.TokenSource(ctx)
	if err != nil {
		event.Outcome = "credential_unavailable"
		writeError(w, &apiError{503, event.Outcome, "Devin credential unavailable; authenticate with Devin CLI"}, false)
		return
	}
	if _, err = validToken(token); err != nil {
		writeError(w, &apiError{503, "credential_unavailable", "invalid Devin credential"}, false)
		return
	}
	up, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encode(req, token)))
	if err != nil {
		writeError(w, &apiError{502, "upstream_error", "could not create upstream request"}, false)
		return
	}
	up.Header.Set("Authorization", "Basic "+token+"-"+token)
	up.Header.Set("Content-Type", "application/connect+proto")
	up.Header.Set("Connect-Protocol-Version", "1")
	up.Header.Set("Connect-Accept-Encoding", "gzip")
	up.Header.Set("User-Agent", "connect-es/2.0.0")
	res, err := h.client.Do(up)
	if err != nil {
		code, status := "upstream_unavailable", 502
		if ctx.Err() != nil {
			code, status = "request_cancelled", 504
		}
		event.Outcome = code
		writeError(w, &apiError{status, code, "Devin connection failed"}, false)
		return
	}
	defer res.Body.Close()
	// Close even a transport whose body blocks in Read after request cancellation.
	stopClose := context.AfterFunc(ctx, func() { res.Body.Close() })
	defer stopClose()
	if res.StatusCode != 200 {
		status := res.StatusCode
		if status != 400 && status != 401 && status != 403 && status != 429 && status != 503 && status != 504 {
			status = 502
		}
		if retry, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && retry >= 0 && retry <= 3600 {
			w.Header().Set("Retry-After", strconv.Itoa(retry))
		}
		event.Outcome = fmt.Sprintf("upstream_http_%d", res.StatusCode)
		writeError(w, &apiError{status, event.Outcome, "Devin upstream rejected request"}, false)
		return
	}
	if !strings.HasPrefix(res.Header.Get("Content-Type"), "application/connect+proto") {
		writeError(w, &apiError{502, "protocol_error", "unexpected Devin response format"}, false)
		return
	}
	state := streamState{byID: map[string]int{}, allowed: map[string]bool{}}
	for _, t := range req.Tools {
		state.allowed[t.Function.Name] = true
	}
	started := false
	created := time.Now().Unix()
	chunk := func(delta map[string]any, finish any) map[string]any {
		return map[string]any{"id": id, "object": "chat.completion.chunk", "created": created, "model": req.Model,
			"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}}
	}
	emit := func(v any) error {
		if !started {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(200)
			started = true
		}
		// Bounded downstream writes, independent of model thinking time.
		rc := http.NewResponseController(w)
		_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
		b, _ := json.Marshal(v)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return err
		}
		if err := rc.Flush(); err != nil {
			return err
		}
		_ = rc.SetWriteDeadline(time.Time{})
		return nil
	}
	reader := io.LimitReader(res.Body, 32<<20)
	decodedBytes := 0
	for {
		var p proto
		var terminal bool
		p, terminal, err = readFrame(reader)
		if err != nil || terminal {
			break
		}
		event.Chunks++
		for _, values := range p {
			for _, v := range values {
				decodedBytes += len(v.b) + 16
			}
		}
		if decodedBytes > 32<<20 {
			err = errWire
			break
		}
		if event.Chunks > 65536 {
			err = errWire
			break
		}
		var delta map[string]any
		delta, err = state.consume(p)
		if err != nil {
			break
		}
		if state.actualModel != "" && state.actualModel != req.Model {
			err = errors.New("unexpected upstream model")
			break
		}
		if len(delta) > 0 {
			if event.FirstDeltaMS == 0 {
				event.FirstDeltaMS = time.Since(start).Milliseconds()
			}
			if req.Stream {
				if !started {
					err = emit(chunk(map[string]any{"role": "assistant"}, nil))
					if err != nil {
						break
					}
				}
				if err = emit(chunk(delta, nil)); err != nil {
					break
				}
			}
		}
	}
	event.Usage = state.usage
	var finish string
	if err == nil {
		finish, err = state.finish(req.MaxTokens)
	}
	if err != nil {
		var ae *apiError
		if !errors.As(err, &ae) {
			ae = &apiError{502, "protocol_error", "Devin stream interrupted or unsupported; response not complete"}
		}
		if ctx.Err() != nil {
			ae = &apiError{504, "request_cancelled", "request cancelled or deadline exceeded"}
		}
		event.Outcome = ae.code
		writeError(w, ae, started)
		return
	}
	if req.Stream {
		if err = emit(chunk(map[string]any{}, finish)); err == nil && req.StreamOptions != nil && req.StreamOptions.IncludeUsage && state.usage != nil {
			last := chunk(nil, nil)
			last["choices"] = []any{}
			last["usage"] = state.usage
			err = emit(last)
		}
		if err == nil {
			rc := http.NewResponseController(w)
			_ = rc.SetWriteDeadline(time.Now().Add(30 * time.Second))
			_, err = io.WriteString(w, "data: [DONE]\n\n")
			if err == nil {
				err = rc.Flush()
			}
		}
	} else {
		msg := map[string]any{"role": "assistant", "content": state.content.String()}
		if state.reasoning.Len() > 0 {
			msg["reasoning_content"] = state.reasoning.String()
		}
		if len(state.calls) > 0 {
			msg["tool_calls"] = state.calls
		}
		out := map[string]any{"id": id, "object": "chat.completion", "created": created, "model": req.Model,
			"choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": finish}}}
		if state.usage != nil {
			out["usage"] = state.usage
		}
		writeJSON(w, 200, out)
	}
	if err != nil {
		event.Outcome = "client_write_failed"
	} else {
		event.Outcome = "complete"
	}
}

type streamState struct {
	content, reasoning strings.Builder
	calls              []ToolCall
	byID               map[string]int
	allowed            map[string]bool
	activeID           string
	finishCode         int64
	hasFinish          bool
	actualModel        string
	usage              *Usage
}

func (s *streamState) consume(p proto) (map[string]any, error) {
	d := map[string]any{}
	if v := p.str(3); v != "" {
		d["content"] = v
		s.content.WriteString(v)
	}
	if v := p.str(9); v != "" {
		d["reasoning_content"] = v
		s.reasoning.WriteString(v)
	}
	if n, ok := p.number(5); ok {
		s.finishCode, s.hasFinish = n, true
	}
	if vs := p[7]; len(vs) > 0 {
		m, err := parse(vs[0].b)
		if err != nil {
			return nil, err
		}
		if v := m.str(9); v != "" {
			s.actualModel = v
		}
		input, hasInput := m.number(2)
		output, hasOutput := m.number(3)
		// Missing counts remain unknown, never invented as zero.
		if hasInput && hasOutput && input <= 1<<40 && output <= 1<<40 {
			s.usage = &Usage{input, output, input + output}
		}
	}
	var deltas []any
	for _, v := range p[6] {
		f, err := parse(v.b)
		if err != nil {
			return nil, err
		}
		id, name, args := f.str(1), f.str(2), f.str(3)
		if len(f[4]) > 0 || len(f[5]) > 0 {
			return nil, errors.New("invalid upstream tool arguments")
		}
		if n, _ := f.number(6); n != 0 {
			return nil, errors.New("custom tool calls unsupported")
		}
		if id == "" {
			id = s.activeID
			if id == "" {
				return nil, errors.New("orphan tool fragment")
			}
			// No guessed association when multiple incomplete calls interleave.
			for _, tc := range s.calls {
				if tc.ID != id && !json.Valid([]byte(tc.Function.Arguments)) {
					return nil, errors.New("ambiguous tool fragment")
				}
			}
		}
		index, exists := s.byID[id]
		if !exists {
			if !s.allowed[name] || len(id) > 256 || len(s.calls) >= 256 {
				return nil, errors.New("unexpected tool call")
			}
			index = len(s.calls)
			s.byID[id] = index
			s.calls = append(s.calls, ToolCall{ID: id, Type: "function", Function: FunctionCall{Name: name}})
		} else if name != "" && name != s.calls[index].Function.Name {
			return nil, errors.New("tool name changed midstream")
		}
		s.activeID = id
		if len(s.calls[index].Function.Arguments)+len(args) > maxFrame {
			return nil, errWire
		}
		s.calls[index].Function.Arguments += args
		fn := map[string]any{"arguments": args}
		delta := map[string]any{"index": index, "function": fn}
		if !exists {
			delta["id"] = id
			delta["type"] = "function"
			fn["name"] = name
		}
		deltas = append(deltas, delta)
	}
	if len(deltas) > 0 {
		d["tool_calls"] = deltas
	}
	return d, nil
}
func (s *streamState) finish(limit int) (string, error) {
	if len(s.calls) > 0 {
		for _, tc := range s.calls {
			var args map[string]any
			if json.Unmarshal([]byte(tc.Function.Arguments), &args) != nil || args == nil {
				return "", errWire
			}
		}
		return "tool_calls", nil
	}
	if !s.hasFinish {
		return "", errWire
	}
	if s.usage != nil && s.usage.Completion == int64(limit) {
		return "length", nil
	}
	// Only SWE's observed normal finish is trusted; do not guess enum meanings.
	if s.finishCode != 2 {
		return "", errWire
	}
	return "stop", nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, e *apiError, stream bool) {
	v := map[string]any{"error": map[string]any{"type": e.code, "code": e.code, "message": e.message}}
	if !stream {
		writeJSON(w, e.status, v)
		return
	}
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Second))
	b, _ := json.Marshal(v)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
	_ = rc.Flush()
	// Deliberately no finish chunk or [DONE] after an interrupted stream.
}
