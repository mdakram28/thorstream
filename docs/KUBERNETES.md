# Kubernetes-First Deployment

Thorstream supports a Kubernetes-first, near-stateless deployment model:

- Brokers run as `Deployment` (not `StatefulSet`) for fast scaling.
- Local state uses `emptyDir` for speed.
- Durable history is mirrored to object-store-backed volume via `THORSTREAM_OBJECT_STORE_DIR`.
- HPA scales broker count by CPU and memory.

## Included manifests

- `deploy/k8s/thorstream-deployment.yaml`
- `deploy/k8s/thorstream-hpa.yaml`

## Apply

```bash
kubectl apply -f deploy/k8s/thorstream-deployment.yaml
kubectl apply -f deploy/k8s/thorstream-hpa.yaml
```

## Object-store-backed durability

Set:

- `THORSTREAM_OBJECT_STORE_DIR=/var/thorstream-object-store`
- `THORSTREAM_OBJECT_STORE_REQUIRED=true`

Behavior:

1. On append, records are mirrored to object-store-backed segment files.
2. On startup, if local segment is empty/missing, broker restores from object-store mirror.

This enables near-stateless brokers with quick pod replacement and lower ops overhead.

## Auto-scaling guidance

- Start with `minReplicas=2` and `maxReplicas=12`.
- Keep broker CPU request realistic (e.g., `250m`+) for stable HPA decisions.
- Use PDB to preserve quorum/availability during node maintenance.

## Production notes

- Back object-store PVC with S3-compatible CSI / gateway or mounted object storage.
- Keep compatibility APIs behind Ingress + TLS/mTLS.
- Use cluster-autoscaler alongside HPA for node elasticity.
- Forward audit logs to centralized logging.
