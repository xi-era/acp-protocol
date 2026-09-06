package acp

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strings"
	"sync"
)

// gzipThresholdBytes is the buffered-response size at or above which the
// server gzips when the client accepts it (spec v0.2 §9.4, suggested 1024).
const gzipThresholdBytes = 1024

// isWSUpgrade reports whether the request is a WebSocket upgrade.
func isWSUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	return strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// Handler returns an http.Handler serving, on a single port:
//   - POST /acp          — the envelope endpoint (MUST, spec §9.1)
//   - GET  /acp/discover — browseable discovery (SHOULD)
//   - GET  /acp/health   — liveness probe (MAY)
//   - ws   /acp          — WebSocket upgrade (spec §10)
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/acp":
			switch {
			case isWSUpgrade(r):
				s.serveWS(w, r)
			case r.Method == http.MethodPost:
				s.serveHTTPPost(w, r)
			default:
				s.writeBuffered(w, r, ErrorEnvelope(nil, CodeMethodNotAllowed,
					fmt.Sprintf("method %s not allowed on /acp", r.Method), nil, ProtocolVersion))
			}
		case "/acp/discover":
			if r.Method == http.MethodGet {
				s.serveHTTPDiscover(w, r)
				return
			}
			writePlainJSON(w, http.StatusNotFound, []byte(`{"ok":false}`))
		case "/acp/health":
			if r.Method == http.MethodGet {
				writePlainJSON(w, http.StatusOK, []byte(`{"ok":true}`))
				return
			}
			writePlainJSON(w, http.StatusNotFound, []byte(`{"ok":false}`))
		default:
			writePlainJSON(w, http.StatusNotFound, []byte(`{"ok":false}`))
		}
	})
}

func writePlainJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// gzipAccepted reports whether the request's Accept-Encoding includes gzip.
func gzipAccepted(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Accept-Encoding")), "gzip")
}

// httpStatusOfFrame maps a failure envelope to its HTTP status (spec §9.3).
func httpStatusOfFrame(f frame) int {
	if isErrFrame(f) {
		if body, ok := errBodyOf(f); ok {
			return ACPCodeToHTTPStatus(body.Code)
		}
	}
	return http.StatusOK
}

// writeBuffered writes a one-shot JSON response, gzipping at/above the
// threshold when the client accepts gzip (spec v0.2 §9.4).
func (s *Server) writeBuffered(w http.ResponseWriter, r *http.Request, f frame) {
	body, err := json.Marshal(f)
	if err != nil {
		writePlainJSON(w, http.StatusInternalServerError, []byte(`{"ok":false}`))
		return
	}
	status := httpStatusOfFrame(f)
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Vary", "Accept-Encoding")
	if gzipThresholdBytes > 0 && len(body) >= gzipThresholdBytes && gzipAccepted(r) {
		h.Set("Content-Encoding", "gzip")
		w.WriteHeader(status)
		gz := gzip.NewWriter(w)
		_, _ = gz.Write(body)
		_ = gz.Close()
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// readJSONBody reads the request body, transparently gunzipping a
// Content-Encoding: gzip body (spec v0.2 §9.4). Other encodings are rejected
// with 41500.
func readJSONBody(r *http.Request) ([]byte, *EnvelopeError) {
	enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	var reader io.Reader = r.Body
	switch {
	case enc == "" || enc == "identity":
		// plain
	case strings.Contains(enc, "gzip"):
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, &EnvelopeError{Code: CodeUnsupportedMediaType, Message: "invalid gzip request body"}
		}
		reader = gz
	default:
		return nil, &EnvelopeError{Code: CodeUnsupportedMediaType,
			Message: fmt.Sprintf("unsupported content-encoding: %s", enc)}
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, &EnvelopeError{Code: CodeParseError, Message: "failed to read request body"}
	}
	return body, nil
}

// serveHTTPPost implements POST /acp: buffered JSON for one-shot replies,
// NDJSON lines for stream:true calls (spec §9.2).
func (s *Server) serveHTTPPost(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		s.writeBuffered(w, r, ErrorEnvelope(nil, CodeUnsupportedMediaType,
			"content-type must be application/json", nil, ProtocolVersion))
		return
	}
	body, verr := readJSONBody(r)
	if verr != nil {
		s.writeBuffered(w, r, ErrorEnvelope(nil, verr.Code, verr.Message, verr.Data, ProtocolVersion))
		return
	}
	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		s.writeBuffered(w, r, ErrorEnvelope(nil, CodeParseError,
			"request body is not valid JSON", nil, ProtocolVersion))
		return
	}

	streaming := false
	if m, ok := parsed.(map[string]any); ok {
		if b, ok := m["stream"].(bool); ok {
			streaming = b
		}
	}

	conn := &Conn{
		Meta: map[string]any{"transport": "http", "ip": r.RemoteAddr},
		Ctx:  r.Context(),
	}
	if streaming {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		var wmu sync.Mutex
		conn.send = func(f frame) error {
			b, err := json.Marshal(f)
			if err != nil {
				return err
			}
			b = append(b, '\n')
			wmu.Lock()
			defer wmu.Unlock()
			if _, err := w.Write(b); err != nil {
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
			return nil
		}
		// Streaming dispatch: the handler writes NDJSON lines as it goes.
		s.Handle(parsed, conn)
		return
	}

	var (
		reply frame
		got   bool
	)
	conn.send = func(f frame) error {
		if !got {
			got, reply = true, f
		}
		return nil
	}
	s.Handle(parsed, conn)
	if !got {
		// Handler sent nothing (should not happen).
		reply = ErrorEnvelope(nil, CodeInternalError, "no reply from dispatcher", nil, ProtocolVersion)
	}
	s.writeBuffered(w, r, reply)
}

// serveHTTPDiscover implements GET /acp/discover: it synthesizes a discover
// envelope and returns the bare result (or the full envelope on failure).
func (s *Server) serveHTTPDiscover(w http.ResponseWriter, r *http.Request) {
	env := frame{"acp": s.opts.protocolVersion, "id": fmt.Sprintf("get-%d", nowMilli()), "op": "discover"}
	var (
		reply frame
		got   bool
	)
	conn := &Conn{Meta: map[string]any{"transport": "http"}, Ctx: r.Context()}
	conn.send = func(f frame) error {
		if !got {
			got, reply = true, f
		}
		return nil
	}
	s.Handle(env, conn)
	if !got {
		reply = ErrorEnvelope(nil, CodeInternalError, "no reply from dispatcher", nil, ProtocolVersion)
	}
	if !isErrFrame(reply) {
		s.writeBuffered(w, r, frame(reply["result"]))
		return
	}
	s.writeBuffered(w, r, reply)
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

// httpTransport is the connectionless HTTP client transport (spec §9): each
// Request is one POST; RequestStream reads the NDJSON body line by line.
type httpTransport struct {
	url     string
	headers http.Header
	hc      *http.Client
}

// newHTTPTransport builds the HTTP client transport for an endpoint URL.
func newHTTPTransport(url string, headers http.Header) *httpTransport {
	return &httpTransport{url: url, headers: headers, hc: &http.Client{}}
}

func (t *httpTransport) post(ctx context.Context, env Envelope, ndjson bool) (*http.Response, error) {
	body, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, vs := range t.headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if ndjson {
		req.Header.Set("Accept", "application/x-ndjson")
	}
	return t.hc.Do(req)
}

// Request performs one POST and decodes the single reply envelope. NDJSON
// bodies (a stream:true sent via the low-level Request) are handled by
// taking the first line.
func (t *httpTransport) Request(ctx context.Context, env Envelope) (map[string]any, error) {
	resp, err := t.post(ctx, env, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	text := string(data)
	ct := resp.Header.Get("Content-Type")
	if strings.Contains(ct, "x-ndjson") {
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var f map[string]any
			if err := json.Unmarshal([]byte(line), &f); err != nil {
				return nil, fmt.Errorf("invalid NDJSON response line: %w", err)
			}
			return f, nil
		}
		return nil, fmt.Errorf("empty NDJSON response")
	}
	var f map[string]any
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("invalid ACP response (HTTP %d): %.120s", resp.StatusCode, text)
	}
	return f, nil
}

// RequestStream performs one POST and iterates the NDJSON reply body.
func (t *httpTransport) RequestStream(ctx context.Context, env Envelope) (iter.Seq2[map[string]any, error], error) {
	resp, err := t.post(ctx, env, true)
	if err != nil {
		return nil, err
	}
	ct := resp.Header.Get("Content-Type")
	seq := func(yield func(map[string]any, error) bool) {
		defer resp.Body.Close()
		if !strings.Contains(ct, "x-ndjson") {
			// Buffered single frame (e.g. an error envelope).
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				yield(nil, err)
				return
			}
			var f map[string]any
			if err := json.Unmarshal(data, &f); err != nil {
				yield(nil, fmt.Errorf("invalid ACP response (HTTP %d): %.120s", resp.StatusCode, string(data)))
				return
			}
			yield(f, nil)
			return
		}
		br := bufio.NewReader(resp.Body)
		for {
			line, err := br.ReadString('\n')
			trimmed := strings.TrimSpace(line)
			if trimmed != "" {
				var f map[string]any
				if jerr := json.Unmarshal([]byte(trimmed), &f); jerr != nil {
					yield(nil, jerr)
					return
				}
				if !yield(f, nil) {
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					yield(nil, err)
				}
				return
			}
		}
	}
	return seq, nil
}

func (t *httpTransport) Close() error { return nil }

func (t *httpTransport) EventsSupported() bool { return false }

func (t *httpTransport) OnEvent(func(Event)) func() { return func() {} }
