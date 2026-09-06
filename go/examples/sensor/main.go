// Command sensor is a minimal ACP 0.2 server example: an HTTP+WS endpoint
// exposing a (simulated) temperature sensor component.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"

	acp "github.com/xi-era/acp-protocol/go/acp"
)

func main() {
	port := flag.String("port", "", "listen port (default 8090, or ACP_PORT env)")
	flag.Parse()
	if *port == "" {
		*port = os.Getenv("ACP_PORT")
	}
	if *port == "" {
		*port = "8090"
	}

	srv := acp.NewServer(acp.ServerOptions{
		Name:                    "edge-node-01",
		Version:                 "1.0.0",
		MaxSubscriptionsPerConn: 64,
		QueueLimit:              256,
	})

	err := srv.Register(acp.Component{
		ID:          "sensor.temperature",
		Name:        "Temperature Sensor",
		Description: "Reads current temperature from a virtual sensor",
		Version:     "1.0.0",
		InputSchema: []byte(`{
			"type": "object",
			"properties": {"unit": {"enum": ["C", "F"]}},
			"required": []
		}`),
		OutputSchema: []byte(`{
			"type": "object",
			"properties": {"celsius": {"type": "number"}},
			"required": ["celsius"]
		}`),
		Tags: []string{"iot", "sensor"},
		Handle: func(ctx context.Context, input any, send acp.Sender) (any, error) {
			unit := "C"
			if m, ok := input.(map[string]any); ok {
				if u, ok := m["unit"].(string); ok && u != "" {
					unit = u
				}
			}
			celsius := 21.5
			if unit == "F" {
				// Report in Fahrenheit while keeping the canonical field.
				return map[string]any{"celsius": celsius, "fahrenheit": celsius*9/5 + 32}, nil
			}
			return map[string]any{"celsius": celsius}, nil
		},
	})
	if err != nil {
		log.Fatalf("register sensor.temperature: %v", err)
	}

	// A streaming component: emits n readings then ends.
	err = srv.Register(acp.Component{
		ID:          "sensor.temperature.stream",
		Name:        "Temperature Stream",
		Description: "Streams n simulated readings as chunks",
		Version:     "1.0.0",
		Stream:      true,
		Tags:        []string{"iot", "sensor", "stream"},
		Handle: func(ctx context.Context, input any, send acp.Sender) (any, error) {
			if send == nil {
				return nil, acp.ErrStreamRequired
			}
			n := 3
			if m, ok := input.(map[string]any); ok {
				if v, ok := m["n"].(float64); ok && v > 0 {
					n = int(v)
				}
			}
			for i := 0; i < n; i++ {
				if err := send(map[string]any{"i": i, "celsius": 21.5}); err != nil {
					return nil, err
				}
			}
			return nil, nil
		},
	})
	if err != nil {
		log.Fatalf("register sensor.temperature.stream: %v", err)
	}

	// Fan out a reading to subscribers every second, demonstrating $events.
	go func() {
		for {
			srv.Emit(acp.Event{Component: "sensor.temperature", Data: map[string]any{"celsius": 21.5}})
		}
	}()

	addr := "127.0.0.1:" + *port
	log.Printf("ACP server listening on http://%s/acp (POST /acp, GET /acp/discover, GET /acp/health, ws /acp)", addr)
	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Fatalf("listen: %v", err)
	}
}
