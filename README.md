# mysqlmcp

A minimal Model Context Protocol (MCP) server for read-only MySQL queries.

## Setup

1. Copy the config template and edit it:

```bash
cp config.example.toml config.toml
```

2. Ensure `mysql.dsn` uses a **read-only MySQL account**.

## Run

```bash
go run . -config config.toml
```

## Tool

- `mysql_query`
  - Input: `{ "query": "SELECT ..." }`
  - Output: `{ "columns": [...], "rows": [...], "rowCount": 3, "truncated": false }`

## Notes

- Only `SELECT`, `SHOW`, `DESCRIBE`, and `EXPLAIN` statements are allowed by default.
- The server enforces a read-only transaction and rejects queries containing semicolons.
- Use `deny_substrings` in TOML to block edge-case write/lock clauses.
- Configure row limits and timeouts via TOML.
