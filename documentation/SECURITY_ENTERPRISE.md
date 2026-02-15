# Enterprise Security

Related docs: [Deployment TLS](DEPLOYMENT_TLS.md) · [Operations](OPERATIONS.md) · [Release Checklist](RELEASE_CHECKLIST.md) · [Docs Index](index.md)

Thorstream includes enterprise security foundations for authentication, authorization, and auditing.

## TLS / mTLS

- For production deployment of the compatibility API, terminate TLS at a reverse proxy as documented in `documentation/DEPLOYMENT_TLS.md`.
- mTLS can be enforced at the reverse proxy layer for compatibility APIs.
- For Kafka/custom binary listeners, use a TCP TLS/mTLS terminator (stunnel, Envoy, HAProxy TCP mode) in front of Thorstream.

## SASL mechanisms

Thorstream provides pluggable SASL credential/token validation primitives for:

- `PLAIN`
- `SCRAM-SHA-256`
- `SCRAM-SHA-512`
- `OAUTHBEARER`

Configuration:

- `THORSTREAM_SASL_PLAIN_USERS` = `alice:secret,bob:pwd`
- `THORSTREAM_SASL_SCRAM_USERS` = `svc:scram-secret`
- `THORSTREAM_SASL_OAUTH_TOKENS` = `token-1,token-2`

## Kafka-compatible ACL model

Thorstream supports Kafka-style ACL semantics with principal, operation, resource type, and permission.

Rule format (`THORSTREAM_ACL_RULES`):

`principal|operation|resource_type|resource_pattern|permission`

- `operation`: `READ`, `WRITE`, `DESCRIBE`, `ALTER`, `CLUSTER_ACTION`
- `resource_type`: `TOPIC`, `GROUP`, `CLUSTER`
- `permission`: `ALLOW`, `DENY`
- `resource_pattern`: exact, wildcard `*`, or prefix wildcard like `payments-*`

Example:

```bash
THORSTREAM_ACL_RULES="alice|WRITE|TOPIC|payments-*|ALLOW;alice|READ|TOPIC|payments-*|ALLOW;ops|DESCRIBE|CLUSTER|*|ALLOW"
THORSTREAM_ACL_DEFAULT_ALLOW=false
```

## RBAC (nice-to-have)

Thorstream includes built-in role bindings for convenience:

- `admin`: broad cluster + topic read/write permissions
- `developer`: topic read/write + cluster describe
- `viewer`: topic read + cluster describe

Bind roles with:

```bash
THORSTREAM_RBAC_BINDINGS="alice=admin;bob=viewer"
```

## Audit logs

All authorization checks can be written as JSON lines with:

```bash
THORSTREAM_AUDIT_LOG_PATH=/var/log/thorstream/audit.log
```

Each event includes:

- timestamp
- principal
- operation
- resource type/name
- allow/deny result
- reason

## Example secure run

```bash
THORSTREAM_DEFAULT_PRINCIPAL=alice \
THORSTREAM_SASL_PLAIN_USERS="alice:secret" \
THORSTREAM_ACL_RULES="alice|WRITE|TOPIC|events-*|ALLOW;alice|READ|TOPIC|events-*|ALLOW;alice|DESCRIBE|CLUSTER|*|ALLOW" \
THORSTREAM_ACL_DEFAULT_ALLOW=false \
THORSTREAM_AUDIT_LOG_PATH=./thorstream-audit.log \
THORSTREAM_COMPAT_API_ADDR=127.0.0.1:8083 \
cargo run --bin thorstream
```
