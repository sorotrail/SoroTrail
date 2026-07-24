# Logging

SoroTrail uses structured logging via the Go standard library's `log/slog` package. All logs include a consistent set of fields to enable effective log aggregation and debugging.

## Log Schema

Every log entry includes the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `request_id` | string | Unique identifier for each HTTP request, used to chain all logs for a single request |
| `route` | string | HTTP path (e.g., `/events`, `/subscriptions/{id}`) |
| `status` | int | HTTP status code |
| `duration_ms` | int | Request duration in milliseconds |
| `error` | string | Error message when present, `null` otherwise |
| `method` | string | HTTP method (GET, POST, etc.) |
| `remote` | string | Remote IP address |

## Request IDs

Request IDs are generated per HTTP request using the following logic:

1. Check for an `X-Request-ID` header in the incoming request
2. If present, use that value
3. If absent, generate a cryptographically secure 16-byte random ID
4. Return the ID in the `X-Request-ID` response header
5. Inject the ID into the request context

The request ID appears in **every log** for that request, making it trivial to debug any issue by finding all logs with the same ID.

## Log Categories

### 1. HTTP Request Logs
Log format: `http request`
- Includes: `request_id`, `route`, `status`, `duration_ms`, `error`, `method`, `remote`
- Example: `http request{request_id="abc123", route="/events", status=200, duration_ms=45, error=null, method="GET", remote="10.0.0.1:54321"}`

### 2. Error Logs
Error logs use the same schema but include an `error` field.
- Example: `querying events{request_id="abc123", error="sql: no rows in result set"}`

### 3. Authentication/Authorization Logs
- Use the same schema with appropriate log messages
- Include `request_id` for chaining

### 4. Audit Logs
- Audit logs include all standard fields plus audit-specific data
- Maintain the `request_id` for correlation

## Logging Best Practices

1. **Always include `request_id`**: Every log message for an HTTP request should include the `request_id` field to enable debugging.

2. **Standardize field names**: Use consistent field names across all log messages.

3. **Include status codes and durations**: HTTP status codes and request durations are valuable for monitoring.

4. **Capture errors at the appropriate level**: Errors should be logged at the Error level with the `error` field populated.

## Example Log Output

```json
{
  "time": "2026-07-24T10:55:00.123Z",
  "level": "INFO",
  "message": "http request",
  "request_id": "abc123def456",
  "route": "/events",
  "status": 200,
  "duration_ms": 45,
  "error": null,
  "method": "GET",
  "remote": "10.0.0.1:54321"
}
```

```json
{
  "time": "2026-07-24T10:56:10.234Z",
  "level": "ERROR",
  "message": "querying events",
  "request_id": "abc123def456",
  "error": "database connection timeout",
  "sql": "SELECT * FROM events WHERE ..."
}
```
