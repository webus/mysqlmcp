package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mcpClient interface {
	Connect(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error)
}

type mcpSession interface {
	CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error)
	Close() error
}

type realClient struct {
	inner *mcp.Client
}

func (c *realClient) Connect(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error) {
	s, err := c.inner.Connect(ctx, t, opts)
	if err != nil {
		return nil, err
	}
	return &realSession{inner: s}, nil
}

type realSession struct {
	inner *mcp.ClientSession
}

func (s *realSession) CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	return s.inner.CallTool(ctx, params)
}

func (s *realSession) Close() error {
	return s.inner.Close()
}

func run(ctx context.Context, args []string, newClient func() mcpClient, newTransport func() mcp.Transport, logger *log.Logger) error {
	fs := flag.NewFlagSet("mcp-client", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	query := fs.String("query", "SELECT 1", "Read-only SQL query to run")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *query == "" {
		return errors.New("-query is required")
	}

	client := newClient()
	session, err := client.Connect(ctx, newTransport(), nil)
	if err != nil {
		return err
	}
	defer session.Close()

	params := &mcp.CallToolParams{
		Name:      "mysql_query",
		Arguments: map[string]any{"query": *query},
	}
	res, err := session.CallTool(ctx, params)
	if err != nil {
		return fmt.Errorf("CallTool failed: %w", err)
	}
	if res.IsError {
		if res.StructuredContent != nil {
			b, err := json.MarshalIndent(res.StructuredContent, "", "  ")
			if err == nil {
				return fmt.Errorf("tool failed: %s", string(b))
			}
		}
		for _, c := range res.Content {
			if t, ok := c.(*mcp.TextContent); ok {
				return fmt.Errorf("tool failed: %s", t.Text)
			}
		}
		return errors.New("tool failed")
	}
	if res.StructuredContent != nil {
		b, err := json.MarshalIndent(res.StructuredContent, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal structured content: %w", err)
		}
		logger.Print(string(b))
		return nil
	}
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			logger.Print(t.Text)
			continue
		}
		b, err := json.MarshalIndent(c, "", "  ")
		if err != nil {
			logger.Printf("(unmarshalable content type %T)", c)
			continue
		}
		logger.Print(string(b))
	}
	return nil
}

func main() {
	ctx := context.Background()
	logger := log.Default()
	err := run(
		ctx,
		os.Args[1:],
		func() mcpClient {
			c := mcp.NewClient(&mcp.Implementation{Name: "mcp-client", Version: "v1.0.0"}, nil)
			return &realClient{inner: c}
		},
		func() mcp.Transport {
			return &mcp.CommandTransport{Command: exec.Command("bin/mysqlmcp")}
		},
		logger,
	)
	if err != nil {
		log.Fatal(err)
	}
}
