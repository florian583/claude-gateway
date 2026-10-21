package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/florian583/claude-gateway/internal/devin"
)

func TestDevinAdapterExample(t *testing.T) {
	const path = "examples/adapters/devin.json"
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeFileConfig(path, b); err != nil {
		t.Fatal(err)
	}
}

type devinTestTransport func(*http.Request) (*http.Response, error)

func (f devinTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDevinAdapterAnthropicTranslation(t *testing.T) {
	str := func(tag byte, s string) []byte {
		return append(binary.AppendUvarint([]byte{tag<<3 | 2}, uint64(len(s))), []byte(s)...)
	}
	frame := func(flag byte, b []byte) []byte {
		return append(binary.BigEndian.AppendUint32([]byte{flag}, uint32(len(b))), b...)
	}
	body := frame(0, str(3, "adapter reply"))
	// Finish=2; prompt=12, completion=4 in response metadata.
	body = append(body, frame(0, []byte{5 << 3, 2, 7<<3 | 2, 4, 2 << 3, 12, 3 << 3, 4})...)
	body = append(body, frame(2, []byte("{}"))...)
	h, err := devin.NewHandler(devin.Config{APIKey: "synthetic-adapter-client-key", TokenSource: func(context.Context) (string, error) { return "synthetic-upstream-key", nil },
		HTTPClient: &http.Client{Transport: devinTestTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/connect+proto"}}, Body: io.NopCloser(bytes.NewReader(body))}, nil
		})}})
	if err != nil {
		t.Fatal(err)
	}
	request, err := anthropicToOpenAIRequest(map[string]any{"model": "swe-2-high", "max_tokens": 128, "stream": true,
		"messages": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "test"}}}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(request))
	r.Header.Set("Authorization", "Bearer synthetic-adapter-client-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("adapter rejected translated request: %s", w.Body)
	}
	var out bytes.Buffer
	if err := translateOpenAIStream(w.Body, &out, "swe-test"); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"adapter reply", `"input_tokens":12`, `"output_tokens":4`, `"stop_reason":"end_turn"`, `"type":"message_stop"`} {
		if !strings.Contains(out.String(), part) {
			t.Fatalf("missing %s in %s", part, &out)
		}
	}
}
