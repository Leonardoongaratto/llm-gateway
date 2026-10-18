# llm-gateway

High-performance AI Gateway with hard budgets, fallbacks, event streaming, and unified metrics built in Go.

> **Status:** Em construção (Fase 1 · Microsserviços em Go — Etapa 1A: Fundação)

---

## Architecture Overview

```
                                      ┌────────────────┐
                                      │   Client App   │
                                      └───────┬────────┘
                                              │ HTTP
                                              ▼
                                      ┌────────────────┐
                                      │   edge :8787   │ (Reverse proxy & TLS termination)
                                      └───────┬────────┘
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      │                                               │
                      ▼                                               ▼
              ┌───────────────┐                               ┌───────────────┐
              │ gateway :8788 │                               │ control :8790 │ (Admin API)
              └───┬───────┬───┘                               └───────┬───────┘
                  │       │                                           │ gRPC :9102
                  │ gRPC  │ JetStream                                 │ (GetSnapshot)
                  │ :9101 │ events                                    │
                  ▼       ▼                                           │
          ┌───────────────┐       ┌───────────────┐                   │
          │ budget :8792  │       │  usage :8791  │◄──────────────────┘
          └───────────────┘       └───────────────┘
```

### Services Summary

| Service | HTTP Port | gRPC Port | Description |
| :--- | :--- | :--- | :--- |
| `edge` | `8787` | — | Reverse proxy, request ID injection, rate-limit check, routing. |
| `gateway` | `8788` | — | Core routing, cache, budget reservation, provider adapters, event publishing. |
| `budget` | `8792` | `9101` | Atomic budget reservation, rate limiting, and balance settlement. |
| `control` | `8790` | `9102` | Key management, pricing configuration, snapshot distribution. |
| `usage` | `8791` | — | Event ingestion (JetStream), audit ledger, analytics, external imports. |

---

## Technical Stack & Libraries

- **Language:** Go 1.27
- **Protobuf & RPC:** Protobuf v3 with `bufbuild/buf`, `google.golang.org/grpc` (v1.84.0) and `google.golang.org/protobuf` (v1.36.12)
- **Messaging:** `github.com/nats-io/nats.go` (v1.54.0) and embedded `github.com/nats-io/nats-server/v2` (v2.15.1) for testing
- **Databases:** `modernc.org/sqlite` (pure Go / no CGO) and `github.com/jackc/pgx/v5` (PostgreSQL)
- **Observability:** OpenTelemetry Go (`go.opentelemetry.io/otel`), OTLP trace exporter, and Prometheus metrics exporter (`go.opentelemetry.io/otel/exporters/prometheus`)
- **Identifiers:** UUIDv7 (`github.com/google/uuid`) for time-ordered request and reservation IDs

---

## Mechanical Gates

Run the automated scripts in `scripts/`:

```powershell
# Format code
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\format.ps1

# Run linters & static checks
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\lint.ps1

# Run tests and verify coverage (threshold ≥ 80%)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\coverage.ps1

# Compile all binaries
go build ./cmd/...

# Execute runtime verification (spawns 5 services, curls health endpoints)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\runtime.ps1
```
