# UI Guide

Thorstream UI is a React + shadcn-style dashboard for operations and ecosystem compatibility workflows.

## Features

- Operational metrics dashboard from `/metrics`
  - under-replicated partitions
  - request latency p99
  - produce/fetch throughput (records and bytes)
  - consumer lag
  - partition size
- Auto-refresh with alert panel for common health signals
- Kafka Connect compatibility workflows
  - plugin catalog
  - connector list/create/pause/resume/delete
- Schema Registry compatibility workflows
  - subject list
  - schema registration
  - compatibility checks

## Run

```bash
cd ui
npm install
npm run dev
```

Default API base in the UI is `/api` via Vite proxy to `http://127.0.0.1:8083`.

## Build

```bash
cd ui
npm run build
npm run preview
```

## Related docs

- [Operations](OPERATIONS.md)
- [Deployment TLS](DEPLOYMENT_TLS.md)
- [Security](SECURITY_ENTERPRISE.md)
