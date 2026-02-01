package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestNormalizeList(t *testing.T) {
	input := []string{"  SELECT ", "", "Show", "  \t", "Describe"}
	got := normalizeList(input)
	require.Equal(t, []string{"select", "show", "describe"}, got)
}

func TestIsReadOnlyQuery(t *testing.T) {
	deny := []string{" into outfile", " for update"}

	cases := []struct {
		name  string
		query string
		ok    bool
	}{
		{"select ok", "SELECT * FROM users", true},
		{"show ok", "show tables", true},
		{"explain ok", "explain select * from users", true},
		{"empty", "   ", false},
		{"multi statement", "select 1; select 2", false},
		{"write prefix", "insert into t values (1)", false},
		{"deny substring", "select * from t for update", false},
		{"outfile", "select * from t into outfile 'x'", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isReadOnlyQuery(tc.query, deny)
			require.Equal(t, tc.ok, got)
		})
	}
}

func TestNormalizeValue(t *testing.T) {
	at := time.Date(2025, 1, 2, 3, 4, 5, 6, time.UTC)
	cases := []struct {
		name string
		in   interface{}
		want interface{}
	}{
		{"nil", nil, nil},
		{"bytes", []byte("hello"), "hello"},
		{"time", at, at.Format(time.RFC3339Nano)},
		{"int", 42, 42},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, normalizeValue(tc.in))
		})
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[mysql]
dsn = "user:pass@tcp(localhost:3306)/db"
`), 0o600))

	cfg, err := loadConfig(path)
	require.NoError(t, err)
	require.Equal(t, "mysql-readonly", cfg.Server.Name)
	require.Equal(t, "v1.0.0", cfg.Server.Version)
	require.Equal(t, []string{"select", "show", "describe", "explain"}, cfg.MySQL.AllowStatementPrefixes)
	require.Equal(t, []string{" into outfile", " into dumpfile", " for update", " lock in share mode"}, cfg.MySQL.DenySubstrings)
}

func TestLoadConfigOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[server]
name = "custom"
version = "v9"

[mysql]
dsn = "user:pass@tcp(localhost:3306)/db"
allow_statement_prefixes = ["select"]
deny_substrings = [" for update"]
`), 0o600))

	cfg, err := loadConfig(path)
	require.NoError(t, err)
	require.Equal(t, "custom", cfg.Server.Name)
	require.Equal(t, "v9", cfg.Server.Version)
	require.Equal(t, []string{"select"}, cfg.MySQL.AllowStatementPrefixes)
	require.Equal(t, []string{" for update"}, cfg.MySQL.DenySubstrings)
}

func TestQueryOutputToStructuredContent_EmptyArrays(t *testing.T) {
	out := QueryOutput{Columns: []string{}, Rows: [][]interface{}{}, RowCount: 0, Truncated: false}
	structured := queryOutputToStructuredContent(out)

	columns, ok := structured["columns"].([]any)
	require.True(t, ok)
	require.NotNil(t, columns)
	require.Len(t, columns, 0)

	rows, ok := structured["rows"].([]any)
	require.True(t, ok)
	require.NotNil(t, rows)
	require.Len(t, rows, 0)
}

func TestQueryOutputToStructuredContent_NonEmpty(t *testing.T) {
	out := QueryOutput{
		Columns: []string{"id", "name"},
		Rows: [][]interface{}{
			{1, "alice"},
			{2, "bob"},
		},
		RowCount:  2,
		Truncated: false,
	}
	structured := queryOutputToStructuredContent(out)

	columns, ok := structured["columns"].([]any)
	require.True(t, ok)
	require.Equal(t, []any{"id", "name"}, columns)

	rows, ok := structured["rows"].([]any)
	require.True(t, ok)
	require.Len(t, rows, 2)
	require.Equal(t, []any{1, "alice"}, rows[0])
	require.Equal(t, []any{2, "bob"}, rows[1])
}

func TestToolErrorResultf_StructuredContentSchema(t *testing.T) {
	result, _ := toolErrorResultf("boom: %s", "nope")
	require.True(t, result.IsError)

	structured, ok := result.StructuredContent.(map[string]any)
	require.True(t, ok)

	columns, ok := structured["columns"].([]any)
	require.True(t, ok)
	require.NotNil(t, columns)

	rows, ok := structured["rows"].([]any)
	require.True(t, ok)
	require.NotNil(t, rows)

	_, ok = result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
}
