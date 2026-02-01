package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Config struct {
	Server struct {
		Name    string `toml:"name"`
		Version string `toml:"version"`
	} `toml:"server"`
	MySQL struct {
		DSN                    string   `toml:"dsn"`
		MaxOpenConns           int      `toml:"max_open_conns"`
		MaxIdleConns           int      `toml:"max_idle_conns"`
		ConnMaxLifetimeSeconds int      `toml:"conn_max_lifetime_seconds"`
		ConnMaxIdleTimeSeconds int      `toml:"conn_max_idle_time_seconds"`
		QueryTimeoutSeconds    int      `toml:"query_timeout_seconds"`
		AllowStatementPrefixes []string `toml:"allow_statement_prefixes"`
		DenySubstrings         []string `toml:"deny_substrings"`
		MaxRows                int      `toml:"max_rows"`
	} `toml:"mysql"`
}

type QueryInput struct {
	Query string `json:"query" jsonschema:"Read-only SQL query (SELECT/SHOW/DESCRIBE/EXPLAIN)."`
}

type QueryOutput struct {
	Columns   []string        `json:"columns" jsonschema:"Column names returned by the query."`
	Rows      [][]interface{} `json:"rows" jsonschema:"Row values for each column."`
	RowCount  int             `json:"rowCount" jsonschema:"Number of rows returned in this response."`
	Truncated bool            `json:"truncated" jsonschema:"True if results were truncated by max_rows."`
}

type queryHandler struct {
	db             *sql.DB
	config         Config
	denySubstrings []string
}

var mysqlIdentifierRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func toolErrorResultf(format string, args ...any) (*mcp.CallToolResult, QueryOutput) {
	output := QueryOutput{
		Columns:   []string{},
		Rows:      [][]interface{}{},
		RowCount:  0,
		Truncated: false,
	}
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf(format, args...)}},
		StructuredContent: queryOutputToStructuredContent(output),
		IsError:           true,
	}, output
}

func queryOutputToStructuredContent(output QueryOutput) map[string]any {
	columns := make([]any, 0, len(output.Columns))
	for _, col := range output.Columns {
		columns = append(columns, col)
	}

	rows := make([]any, 0, len(output.Rows))
	for _, row := range output.Rows {
		rowValues := make([]any, 0, len(row))
		for _, value := range row {
			rowValues = append(rowValues, value)
		}
		rows = append(rows, rowValues)
	}

	return map[string]any{
		"columns":   columns,
		"rows":      rows,
		"rowCount":  output.RowCount,
		"truncated": output.Truncated,
	}
}

func normalizeList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(strings.ToLower(value))
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func isReadOnlyQuery(query string, denySubstrings []string) bool {
	normalized := strings.TrimSpace(strings.ToLower(query))
	if normalized == "" {
		return false
	}
	if strings.Contains(normalized, ";") {
		return false
	}
	for _, fragment := range denySubstrings {
		if fragment != "" && strings.Contains(normalized, fragment) {
			return false
		}
	}
	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return false
	}
	stmt, err := parser.Parse(query)
	if err != nil {
		return false
	}
	switch stmt.(type) {
	case *sqlparser.Select, *sqlparser.Union, *sqlparser.Show, sqlparser.Explain:
		return true
	default:
		return false
	}
}

func (h *queryHandler) runQuery(ctx context.Context, req *mcp.CallToolRequest, input QueryInput) (*mcp.CallToolResult, QueryOutput, error) {
	if !isReadOnlyQuery(input.Query, h.denySubstrings) {
		result, output := toolErrorResultf("only read-only queries are allowed")
		return result, output, nil
	}

	timeout := time.Duration(h.config.MySQL.QueryTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := h.db.Conn(ctx)
	if err != nil {
		result, output := toolErrorResultf("failed to acquire connection: %v", err)
		return result, output, nil
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		result, output := toolErrorResultf("failed to start read-only transaction: %v", err)
		return result, output, nil
	}

	rows, err := tx.QueryContext(ctx, input.Query)
	if err != nil {
		_ = tx.Rollback()
		result, output := toolErrorResultf("query failed: %v", err)
		return result, output, nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		_ = tx.Rollback()
		result, output := toolErrorResultf("failed to fetch columns: %v", err)
		return result, output, nil
	}
	if columns == nil {
		columns = []string{}
	}

	maxRows := h.config.MySQL.MaxRows
	if maxRows <= 0 {
		maxRows = 1000
	}

	results := make([][]interface{}, 0)
	rowCount := 0
	truncated := false
	for rows.Next() {
		if rowCount >= maxRows {
			truncated = true
			break
		}
		values := make([]interface{}, len(columns))
		dest := make([]interface{}, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			_ = tx.Rollback()
			result, output := toolErrorResultf("failed to read row: %v", err)
			return result, output, nil
		}
		for i := range values {
			values[i] = normalizeValue(values[i])
		}
		results = append(results, values)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		result, output := toolErrorResultf("row iteration failed: %v", err)
		return result, output, nil
	}

	if err := tx.Commit(); err != nil {
		result, output := toolErrorResultf("failed to finish transaction: %v", err)
		return result, output, nil
	}

	output := QueryOutput{
		Columns:   columns,
		Rows:      results,
		RowCount:  rowCount,
		Truncated: truncated,
	}
	if output.Columns == nil {
		output.Columns = []string{}
	}
	if output.Rows == nil {
		output.Rows = [][]interface{}{}
	}

	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: "ok"}},
		StructuredContent: queryOutputToStructuredContent(output),
	}, output, nil
}

func (h *queryHandler) runQueryForResource(ctx context.Context, query string) (QueryOutput, error) {
	if !isReadOnlyQuery(query, h.denySubstrings) {
		return QueryOutput{}, fmt.Errorf("only read-only queries are allowed")
	}

	timeout := time.Duration(h.config.MySQL.QueryTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := h.db.Conn(ctx)
	if err != nil {
		return QueryOutput{}, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return QueryOutput{}, fmt.Errorf("failed to start read-only transaction: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		_ = tx.Rollback()
		return QueryOutput{}, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		_ = tx.Rollback()
		return QueryOutput{}, fmt.Errorf("failed to fetch columns: %w", err)
	}
	if columns == nil {
		columns = []string{}
	}

	maxRows := h.config.MySQL.MaxRows
	if maxRows <= 0 {
		maxRows = 1000
	}

	results := make([][]interface{}, 0)
	rowCount := 0
	truncated := false
	for rows.Next() {
		if rowCount >= maxRows {
			truncated = true
			break
		}
		values := make([]interface{}, len(columns))
		dest := make([]interface{}, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			_ = tx.Rollback()
			return QueryOutput{}, fmt.Errorf("failed to read row: %w", err)
		}
		for i := range values {
			values[i] = normalizeValue(values[i])
		}
		results = append(results, values)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return QueryOutput{}, fmt.Errorf("row iteration failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return QueryOutput{}, fmt.Errorf("failed to finish transaction: %w", err)
	}

	output := QueryOutput{
		Columns:   columns,
		Rows:      results,
		RowCount:  rowCount,
		Truncated: truncated,
	}
	if output.Columns == nil {
		output.Columns = []string{}
	}
	if output.Rows == nil {
		output.Rows = [][]interface{}{}
	}

	return output, nil
}

func (h *queryHandler) readResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	uri := req.Params.URI
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(u.Scheme) != "mysql" {
		return nil, mcp.ResourceNotFoundError(uri)
	}

	host := strings.ToLower(u.Host)
	trimmedPath := strings.TrimPrefix(u.Path, "/")
	pathParts := make([]string, 0)
	if trimmedPath != "" {
		pathParts = strings.Split(trimmedPath, "/")
	}

	var query string
	switch host {
	case "databases":
		if len(pathParts) != 0 {
			return nil, mcp.ResourceNotFoundError(uri)
		}
		query = "SHOW DATABASES"
	case "tables":
		if len(pathParts) != 1 {
			return nil, mcp.ResourceNotFoundError(uri)
		}
		db := pathParts[0]
		if !mysqlIdentifierRE.MatchString(db) {
			return nil, mcp.ResourceNotFoundError(uri)
		}
		query = fmt.Sprintf("SHOW TABLES FROM `%s`", db)
	case "schema":
		if len(pathParts) != 2 {
			return nil, mcp.ResourceNotFoundError(uri)
		}
		db := pathParts[0]
		table := pathParts[1]
		if !mysqlIdentifierRE.MatchString(db) || !mysqlIdentifierRE.MatchString(table) {
			return nil, mcp.ResourceNotFoundError(uri)
		}
		query = fmt.Sprintf("DESCRIBE `%s`.`%s`", db, table)
	default:
		return nil, mcp.ResourceNotFoundError(uri)
	}

	out, err := h.runQueryForResource(ctx, query)
	if err != nil {
		return nil, err
	}

	encoded, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}

	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      uri,
			MIMEType: "application/json",
			Text:     string(encoded),
		}},
	}, nil
}

func normalizeValue(value interface{}) interface{} {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	default:
		return v
	}
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Server.Name == "" {
		cfg.Server.Name = "mysql-readonly"
	}
	if cfg.Server.Version == "" {
		cfg.Server.Version = "v1.0.0"
	}
	if len(cfg.MySQL.AllowStatementPrefixes) == 0 {
		cfg.MySQL.AllowStatementPrefixes = []string{"select", "show", "describe", "explain"}
	}
	if len(cfg.MySQL.DenySubstrings) == 0 {
		cfg.MySQL.DenySubstrings = []string{" into outfile", " into dumpfile", " for update", " lock in share mode"}
	}
	return cfg, nil
}

func main() {
	configPath := flag.String("config", "config.toml", "path to TOML config")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config %q: %v\n", *configPath, err)
		os.Exit(1)
	}

	if cfg.MySQL.DSN == "" {
		fmt.Fprintln(os.Stderr, "mysql.dsn is required in config")
		os.Exit(1)
	}

	db, err := sql.Open("mysql", cfg.MySQL.DSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open mysql connection: %v\n", err)
		os.Exit(1)
	}

	if cfg.MySQL.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MySQL.MaxOpenConns)
	}
	if cfg.MySQL.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MySQL.MaxIdleConns)
	}
	if cfg.MySQL.ConnMaxLifetimeSeconds > 0 {
		db.SetConnMaxLifetime(time.Duration(cfg.MySQL.ConnMaxLifetimeSeconds) * time.Second)
	}
	if cfg.MySQL.ConnMaxIdleTimeSeconds > 0 {
		db.SetConnMaxIdleTime(time.Duration(cfg.MySQL.ConnMaxIdleTimeSeconds) * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(ctx); err != nil {
		cancel()
		fmt.Fprintf(os.Stderr, "failed to connect to mysql: %v\n", err)
		os.Exit(1)
	}
	cancel()

	handler := &queryHandler{
		db:             db,
		config:         cfg,
		denySubstrings: normalizeList(cfg.MySQL.DenySubstrings),
	}

	server := mcp.NewServer(&mcp.Implementation{Name: cfg.Server.Name, Version: cfg.Server.Version}, nil)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "mysql_query",
		Description: "Run a read-only SQL query against MySQL.",
	}, handler.runQuery)

	server.AddResource(&mcp.Resource{
		Name:        "mysql_databases",
		URI:         "mysql://databases",
		Description: "List databases available on this MySQL server.",
		MIMEType:    "application/json",
	}, handler.readResource)

	server.AddResourceTemplate(&mcp.ResourceTemplate{
		Name:        "mysql_tables",
		URITemplate: "mysql://tables/{db}",
		Description: "List tables in the given database.",
		MIMEType:    "application/json",
	}, handler.readResource)

	server.AddResourceTemplate(&mcp.ResourceTemplate{
		Name:        "mysql_schema",
		URITemplate: "mysql://schema/{db}/{table}",
		Description: "Describe a table's schema (DESCRIBE).",
		MIMEType:    "application/json",
	}, handler.readResource)

	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatal(err)
	}
}
