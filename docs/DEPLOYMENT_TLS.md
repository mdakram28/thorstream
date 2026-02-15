# Deployment: Compatibility API behind Reverse Proxy + TLS

Related docs: [Operations](OPERATIONS.md) · [Security](SECURITY_ENTERPRISE.md) · [Kubernetes](KUBERNETES.md) · [Docs Index](README.md)

This guide deploys Thorstream with Kafka protocol and compatibility API, while terminating TLS at a reverse proxy.

## Goal architecture

- Thorstream Kafka wire protocol: `127.0.0.1:9093`
- Thorstream compatibility API (Connect + Schema Registry): `127.0.0.1:8083`
- Public HTTPS endpoint for compatibility API: `https://compat.example.com`
- TLS termination at NGINX (or compatible proxy)

## 1) Run Thorstream on loopback interfaces

```bash
THORSTREAM_ADDR=127.0.0.1:9092 \
THORSTREAM_KAFKA_ADDR=127.0.0.1:9093 \
THORSTREAM_COMPAT_API_ADDR=127.0.0.1:8083 \
RUST_LOG=info \
cargo run --bin thorstream
```

For production, run the binary directly or via systemd rather than `cargo run`.

## 2) NGINX TLS reverse proxy for compatibility API

Example NGINX server block:

```nginx
server {
    listen 443 ssl http2;
    server_name compat.example.com;

    ssl_certificate     /etc/letsencrypt/live/compat.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/compat.example.com/privkey.pem;

    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_prefer_server_ciphers on;
    ssl_session_timeout 1d;
    ssl_session_cache shared:TLSCache:10m;

    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;

    location / {
        proxy_pass http://127.0.0.1:8083;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_read_timeout 120s;
    }
}

server {
    listen 80;
    server_name compat.example.com;
    return 301 https://$host$request_uri;
}
```

Reload NGINX:

```bash
sudo nginx -t && sudo systemctl reload nginx
```

## 3) systemd service for Thorstream

Example unit file at `/etc/systemd/system/thorstream.service`:

```ini
[Unit]
Description=Thorstream broker
After=network.target

[Service]
Type=simple
User=thorstream
Group=thorstream
WorkingDirectory=/opt/thorstream
Environment=RUST_LOG=info
Environment=THORSTREAM_ADDR=127.0.0.1:9092
Environment=THORSTREAM_KAFKA_ADDR=127.0.0.1:9093
Environment=THORSTREAM_COMPAT_API_ADDR=127.0.0.1:8083
ExecStart=/opt/thorstream/target/release/thorstream
Restart=always
RestartSec=2
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable thorstream
sudo systemctl start thorstream
sudo systemctl status thorstream
```

## 4) Validation checks

Compatibility API over TLS:

```bash
curl -sSf https://compat.example.com/connector-plugins | jq
curl -sSf https://compat.example.com/subjects | jq
```

Kafka protocol listener (local smoke):

```bash
nc -zv 127.0.0.1 9093
```

## 5) Hardening recommendations

- Keep Thorstream listeners on loopback if a proxy is fronting traffic.
- Restrict firewall ingress to only required public ports (`443` and optionally Kafka listener if exposed).
- Run Thorstream as non-root user.
- Rotate TLS certificates automatically (e.g., certbot timer).
- Centralize logs and alert on restart loops.
- Backup `data/` with filesystem snapshots.

## 6) Optional: expose Kafka listener with TLS proxy

Kafka protocol is binary and typically terminated by Kafka-aware proxies or stunnel/TCP TLS terminators. If you expose Kafka externally, use a TCP TLS proxy and network ACLs.
