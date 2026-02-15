# Thorstream Documentation

Thorstream is a Rust event streaming broker with Kafka-like behavior and compatibility APIs.

## Start here

- [Architecture](ARCHITECTURE.md)
- [Operations](OPERATIONS.md)
- [Deployment TLS](DEPLOYMENT_TLS.md)
- [Kubernetes](KUBERNETES.md)
- [Security](SECURITY_ENTERPRISE.md)
- [Release Checklist](RELEASE_CHECKLIST.md)
- [UI Guide](UI.md)

## Quick commands

Run broker with compatibility API:

```bash
THORSTREAM_COMPAT_API_ADDR=127.0.0.1:8083 cargo run --bin thorstream
```

Run React UI:

```bash
cd ui
npm install
npm run dev
```
