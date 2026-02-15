# Thorstream UI

React + shadcn-style dashboard UI for Thorstream operations.

## Features

- shadcn-style dashboard cards, tables, tabs, and forms
- Prometheus metrics view (`/metrics`) with:
  - under-replicated partitions
  - request latency p99
  - produce/fetch throughput totals (records and bytes)
  - consumer lag table
  - partition size table
- Auto-refresh polling for operational visibility
- Alert panel for common broker conditions (URP, lag, high p99)
- Kafka Connect compatibility operations:
  - plugin catalog
  - list connectors
  - create connector
  - pause/resume/delete connector
- Schema Registry compatibility operations:
  - list subjects
  - register schema versions
  - check compatibility against latest schema

## Requirements

- Node.js 18+
- Running Thorstream broker with compatibility API enabled

Example broker startup:

```bash
THORSTREAM_COMPAT_API_ADDR=127.0.0.1:8083 cargo run --bin thorstream
```

## Run locally

From repository root:

```bash
cd ui
npm install
npm run dev
```

Open `http://127.0.0.1:5173`.

By default, the UI uses Vite proxy (`/api`) -> `http://127.0.0.1:8083`.

To point proxy to a different API address:

```bash
THORSTREAM_COMPAT_API_URL=http://127.0.0.1:18083 npm run dev
```

## Build

```bash
cd ui
npm run build
npm run preview
```

## Stack

- React + Vite
- Tailwind CSS
- shadcn-style reusable UI components with Radix primitives

## Notes

- API base is editable in the UI and persisted in local storage.
- For production deployments, serve `ui/dist` behind a reverse proxy that routes API requests to Thorstream compatibility API.
- Related docs: [../docs/README.md](../docs/README.md), [../README.md](../README.md)
