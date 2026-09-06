//go:build interop

// Cross-language interop: Go AcpClient against the TypeScript ACP server
// (scripts/interop/ts-server.mjs, spawned here). Run with:
//
//	go test -tags interop -run TestInterop -v ./...
package acp

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func waitForPort(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("port %d never became ready", port)
}

func TestInteropWithTSServer(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not available")
	}
	const port = 8613
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	cmd := exec.Command("node", filepath.Join(repoRoot, "scripts", "interop", "ts-server.mjs"), fmt.Sprint(port))
	// NB: (*testing.T).Output() only exists since Go 1.26; CI pins 1.23.
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn ts-server: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	waitForPort(t, port, 15*time.Second)

	ctx := context.Background()
	cli, err := NewClient(fmt.Sprintf("http://127.0.0.1:%d/acp", port), ClientOptions{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cli.Close()

	// discover
	comps, err := cli.Discover(ctx, "")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	found := false
	for _, c := range comps {
		if c.ID == "interop.echo" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing interop.echo; got %d components", len(comps))
	}

	// call (bare result)
	var out struct {
		Msg string `json:"msg"`
	}
	if err := cli.Call(ctx, "interop.echo", map[string]any{"msg": "hola"}, &out); err != nil {
		t.Fatalf("call: %v", err)
	}
	if out.Msg != "hola" {
		t.Fatalf("unexpected result %+v", out)
	}

	// stream (seq + end)
	seq, err := cli.CallStream(ctx, "interop.counter", map[string]any{"n": 3})
	if err != nil {
		t.Fatalf("call_stream: %v", err)
	}
	n := 0
	for chunk, err := range seq {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
		if chunk.Seq != n {
			t.Fatalf("unexpected seq %d (want %d)", chunk.Seq, n)
		}
		n++
	}
	if n != 4 {
		t.Fatalf("expected 4 chunks (3 data + end), got %d", n)
	}
	t.Log("go-client <-> ts-server: PASS")
}
