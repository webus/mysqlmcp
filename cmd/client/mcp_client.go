package main

import (
	"context"
	"encoding/json"
	"log"
	"os/exec"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	ctx := context.Background()
	client := mcp.NewClient(&mcp.Implementation{Name: "mcp-client", Version: "v1.0.0"}, nil)
	transport := &mcp.CommandTransport{Command: exec.Command("bin/mysqlmcp")}
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()
	params := &mcp.CallToolParams{
		Name:      "mysql_query",
		Arguments: map[string]any{"query": "SELECT 1"},
	}
	res, err := session.CallTool(ctx, params)
	if err != nil {
		log.Fatalf("CallTool failed: %v", err)
	}
	if res.IsError {
		log.Fatal("tool failed")
	}
	if res.StructuredContent != nil {
		b, err := json.MarshalIndent(res.StructuredContent, "", "  ")
		if err != nil {
			log.Fatalf("failed to marshal structured content: %v", err)
		}
		log.Print(string(b))
		return
	}
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			log.Print(t.Text)
			continue
		}
		b, err := json.MarshalIndent(c, "", "  ")
		if err != nil {
			log.Printf("(unmarshalable content type %T)", c)
			continue
		}
		log.Print(string(b))
	}
}
