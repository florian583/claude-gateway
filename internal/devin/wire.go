package devin

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
)

const maxFrame = 2 << 20

var errWire = errors.New("invalid or unsupported Devin stream frame")

func vi(n uint64) []byte          { return binary.AppendUvarint(nil, n) }
func pv(tag int, n uint64) []byte { return append(vi(uint64(tag<<3)), vi(n)...) }
func pb(tag int, b []byte) []byte {
	return bytes.Join([][]byte{vi(uint64(tag<<3 | 2)), vi(uint64(len(b))), b}, nil)
}
func ps(tag int, s string) []byte { return pb(tag, []byte(s)) }
func pd(tag int, n float64) []byte {
	return binary.LittleEndian.AppendUint64(vi(uint64(tag<<3|1)), math.Float64bits(n))
}
func join(b ...[]byte) []byte { return bytes.Join(b, nil) }
func nonce(size int) string {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		panic("system randomness unavailable")
	}
	return hex.EncodeToString(b)
}
func uuid() string {
	s := nonce(16)
	return s[:8] + "-" + s[8:12] + "-4" + s[13:16] + "-a" + s[17:20] + "-" + s[20:]
}

type value struct {
	wire byte
	b    []byte
	n    uint64
}
type proto map[int][]value

func parse(b []byte) (proto, error) {
	p := proto{}
	for len(b) > 0 {
		k, n := binary.Uvarint(b)
		if n <= 0 || k>>3 == 0 || k>>3 > (1<<29)-1 {
			return nil, errWire
		}
		b = b[n:]
		v := value{wire: byte(k & 7)}
		switch v.wire {
		case 0:
			v.n, n = binary.Uvarint(b)
			if n <= 0 {
				return nil, errWire
			}
			b = b[n:]
		case 1, 2, 5:
			size := uint64(8)
			if v.wire == 5 {
				size = 4
			}
			if v.wire == 2 {
				size, n = binary.Uvarint(b)
				if n <= 0 {
					return nil, errWire
				}
				b = b[n:]
			}
			if size > uint64(len(b)) {
				return nil, errWire
			}
			v.b = b[:int(size)]
			b = b[int(size):]
		default:
			return nil, errWire
		}
		p[int(k>>3)] = append(p[int(k>>3)], v)
	}
	return p, nil
}
func (p proto) str(tag int) string {
	if vs := p[tag]; len(vs) > 0 && vs[0].wire == 2 {
		return string(vs[0].b)
	}
	return ""
}
func (p proto) number(tag int) (int64, bool) {
	vs := p[tag]
	if len(vs) == 0 || vs[0].wire != 0 || vs[0].n > math.MaxInt64 {
		return 0, false
	}
	return int64(vs[0].n), true
}

func encode(r Request, token string) []byte {
	meta := join(ps(1, "chisel"), ps(2, "3000.10.21"), ps(3, token), ps(4, "en"),
		ps(5, "darwin"), ps(7, "3000.10.21"), ps(12, "chisel"), ps(31, nonce(366)))
	var system string
	var messages []byte
	for _, m := range r.Messages {
		if m.Role == "system" || m.Role == "developer" {
			if system != "" {
				system += "\n\n"
			}
			system += m.text
			continue
		}
		source := map[string]uint64{"user": 1, "assistant": 2, "tool": 4}[m.Role]
		cm := join(ps(1, uuid()), pv(2, source), ps(3, m.text))
		if m.ToolCallID != "" {
			cm = append(cm, ps(7, m.ToolCallID)...)
		}
		for _, tc := range m.ToolCalls {
			cm = append(cm, pb(6, join(ps(1, tc.ID), ps(2, tc.Function.Name), ps(3, tc.Function.Arguments)))...)
		}
		messages = append(messages, pb(3, cm)...)
	}
	if system == "" && len(r.Tools) > 0 {
		system = "Use the provided tools when appropriate."
	}
	temp, top := 1.0, .95
	if r.Temperature != nil {
		temp = *r.Temperature
	}
	if r.TopP != nil {
		top = *r.TopP
	}
	cfg := join(pv(1, 1), pv(2, uint64(r.MaxTokens)), pv(3, 262144), pd(5, temp), pv(7, 40), pd(8, top))
	body := join(pb(1, meta), ps(2, system), messages, pv(7, 5), pb(8, cfg))
	for _, t := range r.Tools {
		body = append(body, pb(10, join(ps(1, t.Function.Name), ps(2, t.Function.Description), pb(3, t.Function.Parameters)))...)
	}
	body = append(body, join(pb(15, join(ps(1, uuid()), pv(2, 1), pv(3, 4))), ps(16, uuid()), pv(20, 1), ps(21, r.Model))...)
	return append(binary.BigEndian.AppendUint32([]byte{0}, uint32(len(body))), body...)
}

// readFrame bounds both compressed and decompressed sizes. A Connect terminal
// envelope is mandatory; HTTP EOF is never translated into a successful finish.
func readFrame(r io.Reader) (proto, bool, error) {
	var h [5]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, false, errWire
	}
	if h[0]&^byte(3) != 0 {
		return nil, false, errWire
	}
	n := binary.BigEndian.Uint32(h[1:])
	if n > maxFrame {
		return nil, false, errWire
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, false, errWire
	}
	if h[0]&1 != 0 {
		z, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, false, errWire
		}
		b, err = io.ReadAll(io.LimitReader(z, maxFrame+1))
		z.Close()
		if err != nil || len(b) > maxFrame {
			return nil, false, errWire
		}
	}
	if h[0]&2 != 0 {
		var t struct {
			Error *struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(b, &t) != nil {
			return nil, false, errWire
		}
		if t.Error != nil {
			if t.Error.Code == "permission_denied" && strings.Contains(strings.ToLower(t.Error.Message), "mcp configuration") {
				return nil, false, &apiError{403, "tool_configuration_rejected", "Devin rejected this tool configuration"}
			}
			return nil, false, rpcError(t.Error.Code)
		}
		return nil, true, nil
	}
	p, err := parse(b)
	return p, false, err
}

type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.message }
func rpcError(code string) error {
	status := 502
	switch code {
	case "unauthenticated":
		status = 401
	case "permission_denied":
		status = 403
	case "resource_exhausted":
		status = 429
	case "invalid_argument", "out_of_range":
		status = 400
	case "deadline_exceeded":
		status = 504
	case "unavailable":
		status = 503
	default:
		code = "upstream_error"
	}
	// Never echo upstream free text: it may contain prompts, tokens or account data.
	return &apiError{status, code, fmt.Sprintf("Devin request failed (%s)", code)}
}
