// Package mcpserver implements a minimal Model Context Protocol server over
// stdio for the open-core pg-cdc edition.
//
// Scope (intentional ceiling — see commercial edition for governance):
//   - Single-user (the local OS user running the binary)
//   - Read-only (no writes back to Postgres)
//   - stdio transport only (no network listener, no auth)
//
// The protocol surface is JSON-RPC 2.0 with newline-delimited messages on
// stdin/stdout, per the MCP stdio transport spec.
package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/pkg/version"
)

// protocolVersion is the MCP spec date this server targets.
const protocolVersion = "2024-11-05"

// ServeStdio runs the MCP server on os.Stdin/os.Stdout until ctx is cancelled
// or the input stream closes.
func ServeStdio(ctx context.Context, cfg config.Config, sink ports.Sink) error {
	s := &server{cfg: cfg, sink: sink}
	return s.serve(ctx, os.Stdin, os.Stdout)
}

type server struct {
	cfg  config.Config
	sink ports.Sink
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (s *server) serve(ctx context.Context, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	enc := json.NewEncoder(out)

	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var req rpcRequest
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			_ = enc.Encode(rpcResponse{
				JSONRPC: "2.0",
				Error:   &rpcError{Code: -32700, Message: "parse error: " + err.Error()},
			})
			continue
		}
		// Notifications carry no id and expect no response.
		if len(req.ID) == 0 {
			s.handleNotification(ctx, req)
			continue
		}
		resp := s.handle(ctx, req)
		if err := enc.Encode(resp); err != nil {
			return fmt.Errorf("encode response: %w", err)
		}
	}
	return scanner.Err()
}

func (s *server) handleNotification(_ context.Context, req rpcRequest) {
	// Currently we have nothing to do for notifications. "notifications/initialized"
	// is the common one — we accept it silently.
	_ = req
}

func (s *server) handle(ctx context.Context, req rpcRequest) rpcResponse {
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
	switch req.Method {
	case "initialize":
		resp.Result = map[string]any{
			"protocolVersion": protocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo": map[string]any{
				"name":    "pg-cdc",
				"version": version.Version,
			},
		}
	case "ping":
		resp.Result = map[string]any{}
	case "tools/list":
		resp.Result = map[string]any{"tools": toolDefinitions()}
	case "tools/call":
		resp.Result = s.callTool(ctx, req.Params)
	default:
		resp.Error = &rpcError{Code: -32601, Message: "method not found: " + req.Method}
	}
	return resp
}
