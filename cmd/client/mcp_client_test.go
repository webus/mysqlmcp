package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type fakeClient struct {
	connect func(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error)
}

func (c *fakeClient) Connect(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error) {
	if c.connect == nil {
		return nil, errors.New("connect not implemented")
	}
	return c.connect(ctx, t, opts)
}

type fakeSession struct {
	callTool    func(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error)
	closeCalled bool
}

func (s *fakeSession) CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	if s.callTool == nil {
		return nil, errors.New("callTool not implemented")
	}
	return s.callTool(ctx, params)
}

func (s *fakeSession) Close() error {
	s.closeCalled = true
	return nil
}

func TestRun_EmptyQueryErrors(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	err := run(ctx, []string{"-query", ""}, func() mcpClient {
		return &fakeClient{}
	}, func() mcp.Transport {
		return nil
	}, logger)

	require.Error(t, err)
	require.Equal(t, "-query is required", err.Error())
}

func TestRun_ConnectErrorPropagates(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	boom := errors.New("boom")
	err := run(ctx, []string{"-query", "SELECT 1"}, func() mcpClient {
		return &fakeClient{connect: func(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error) {
			return nil, boom
		}}
	}, func() mcp.Transport {
		return nil
	}, logger)

	require.ErrorIs(t, err, boom)
}

func TestRun_CallToolErrorWraps(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	callErr := errors.New("call failed")
	sess := &fakeSession{callTool: func(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
		require.Equal(t, "mysql_query", params.Name)
		require.Equal(t, map[string]any{"query": "SELECT 42"}, params.Arguments)
		return nil, callErr
	}}

	err := run(ctx, []string{"-query", "SELECT 42"}, func() mcpClient {
		return &fakeClient{connect: func(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error) {
			return sess, nil
		}}
	}, func() mcp.Transport {
		return nil
	}, logger)

	require.True(t, sess.closeCalled)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CallTool failed:")
	require.ErrorIs(t, err, callErr)
}

func TestRun_ToolIsError_PrefersStructuredContent(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	sess := &fakeSession{callTool: func(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			IsError:           true,
			StructuredContent: map[string]any{"message": "nope"},
		}, nil
	}}

	err := run(ctx, []string{"-query", "SELECT 1"}, func() mcpClient {
		return &fakeClient{connect: func(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error) {
			return sess, nil
		}}
	}, func() mcp.Transport {
		return nil
	}, logger)

	require.True(t, sess.closeCalled)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tool failed:")
	require.Contains(t, err.Error(), "\"message\": \"nope\"")
}

func TestRun_ToolIsError_UsesTextContentWhenNoStructured(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	sess := &fakeSession{callTool: func(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: "bad"}},
		}, nil
	}}

	err := run(ctx, []string{"-query", "SELECT 1"}, func() mcpClient {
		return &fakeClient{connect: func(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error) {
			return sess, nil
		}}
	}, func() mcp.Transport {
		return nil
	}, logger)

	require.True(t, sess.closeCalled)
	require.Error(t, err)
	require.Equal(t, "tool failed: bad", err.Error())
}

func TestRun_StructuredContentSuccess_LogsJSON(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	sess := &fakeSession{callTool: func(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			StructuredContent: map[string]any{"ok": true},
			Content:           nil,
		}, nil
	}}

	err := run(ctx, []string{"-query", "SELECT 1"}, func() mcpClient {
		return &fakeClient{connect: func(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error) {
			return sess, nil
		}}
	}, func() mcp.Transport {
		return nil
	}, logger)

	require.NoError(t, err)
	require.True(t, sess.closeCalled)
	require.Contains(t, buf.String(), "\"ok\": true")
}

func TestRun_TextContentSuccess_LogsText(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	sess := &fakeSession{callTool: func(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "hello"}},
		}, nil
	}}

	err := run(ctx, []string{"-query", "SELECT 1"}, func() mcpClient {
		return &fakeClient{connect: func(ctx context.Context, t mcp.Transport, opts *mcp.ClientSessionOptions) (mcpSession, error) {
			return sess, nil
		}}
	}, func() mcp.Transport {
		return nil
	}, logger)

	require.NoError(t, err)
	require.True(t, sess.closeCalled)
	require.Contains(t, buf.String(), "hello")
}
