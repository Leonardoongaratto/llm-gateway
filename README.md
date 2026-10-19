# LLM Gateway

High-Performance, Multi-Tenant AI Gateway with Hard Budget Enforcement, Automatic Provider Failover, Distributed Rate Limiting, and Event-Driven Audit Ledger built in Go.

[![Go Version](https://img.shields.io/badge/go-1.27-blue.svg)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Coverage](https://img.shields.io/badge/coverage-%E2%89%A588%25-brightgreen.svg)](#mechanical-gates)

---

## Architecture Overview

LLM Gateway is designed around a microservices architecture where each service owns its domain and database. The proxy edge layer presents a unified interface to client applications, completely transparent to standard OpenAI and Anthropic SDKs.

```mermaid
flowchart TD
    Client["Client Application / SDK"] -->|"HTTP :8787"| Edge["Edge Reverse Proxy (:8787)"]

    subgraph Core Routing & Execution
        Edge -->|"/v1/*"| Gateway1["Gateway #1 (:8788)"]
        Edge -->|"/v1/*"| Gateway2["Gateway #2 (:8789)"]
        Edge -->|"/dashboard*"| StaticSPA["SPA Static Web Assets"]
    end

    subgraph Administration & Telemetry
        Edge -->|"/admin/keys*, /admin/pricing*"| Control["Control Service (:8790)"]
        Edge -->|"/admin/usage*, /admin/summary*"| Usage["Usage Service (:8791)"]
        Edge -->|"/admin/limits*"| Budget["Budget Service (:8792)"]
    end

    subgraph Synchronous Policy & Limits
        Gateway1 -->|"gRPC :9101 (Reserve / Settle)"| Budget
        Gateway2 -->|"gRPC :9101 (Reserve / Settle)"| Budget
        Control -->|"gRPC :9102 (Snapshots)"| Gateway1
        Control -->|"gRPC :9102 (Snapshots)"| Gateway2
    end

    subgraph Upstream Providers
        Gateway1 -->|"HTTP Fallback / Retry"| Providers["LLM Providers (OpenAI, Anthropic, Gemini)"]
        Gateway2 -->|"HTTP Fallback / Retry"| Providers
    end

    subgraph Asynchronous Event Bus
        Gateway1 -.->|"Publish (events.v1.llm_call)"| NATS["NATS JetStream (At-least-once)"]
        Gateway2 -.->|"Publish (events.v1.llm_call)"| NATS
        NATS -.->|"Durable Consumer"| Usage
    end

    subgraph Storage Isolation
        Budget --- BudgetDB[("Budget DB (SQLite / Postgres)")]
        Control --- ControlDB[("Control DB (SQLite / Postgres)")]
        Usage --- UsageDB[("Usage DB (SQLite / Postgres)")]
    end
```

### Microservices Summary

| Service | HTTP Port | gRPC Port | Primary Responsibility |
| :--- | :--- | :--- | :--- |
| **`edge`** | `8787` | — | Reverse proxy, request ID injection, rate limit aggregation, routing, SPA static asset server. |
| **`gateway`** | `8788` | — | Core LLM routing, cache, budget reservation client, provider adapters, event publishing. |
| **`budget`** | `8792` | `9101` | Atomic budget reservations, distributed rate limiting, and balance settlement. |
| **`control`** | `8790` | `9102` | Virtual key management, model pricing configuration, snapshot distribution. |
| **`usage`** | `8791` | — | JetStream event consumption, audit ledger, analytics, external cost importers (Claude Code, etc.). |
| **`lgw-dev`**| `8787` (edge) | `9101, 9102` | All-in-one local development cluster with embedded NATS and ephemeral SQLite. |

---

## Quickstart

### Option 1: All-in-One Local Dev Server (`lgw-dev`)

For local development or testing without any external infrastructure dependencies (no Docker, Postgres, or NATS required):

```bash
# Clone the repository
git clone https://github.com/Leonardoongaratto/llm-gateway.git
cd llm-gateway

# Start all 5 services with embedded NATS JetStream and mock providers
go run ./cmd/lgw-dev --mock-provider --seed-demo
```

The gateway edge is now running on `http://127.0.0.1:8787` with a default seeded API key: `lgw_0123456789abcdef0123456789abcdef`.

Verify cluster health:
```bash
curl -i http://127.0.0.1:8787/health
```

### Option 2: Production Containerized Stack (`Docker Compose`)

To run the complete production-grade stack including PostgreSQL 16, NATS JetStream, Prometheus, and Jaeger:

```bash
docker compose -f deploy/compose.yaml up -d
```

- **Edge Proxy:** `http://localhost:8787`
- **Prometheus Metrics:** `http://localhost:9090`
- **Jaeger Tracing UI:** `http://localhost:16686`

---

## SDK Integration Examples

LLM Gateway provides 100% wire compatibility with standard OpenAI and Anthropic SDKs.

### Python

#### OpenAI SDK

```python
import os
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8787/v1",
    api_key="lgw_0123456789abcdef0123456789abcdef"
)

# Unary Chat Completion
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Explain microservices architecture."}]
)
print(response.choices[0].message.content)

# Streaming SSE Chat Completion
stream = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Count from 1 to 5."}],
    stream=True
)
for chunk in stream:
    if chunk.choices[0].delta.content is not None:
        print(chunk.choices[0].delta.content, end="", flush=True)
```

#### Anthropic SDK

```python
from anthropic import Anthropic

client = Anthropic(
    base_url="http://localhost:8787",
    api_key="lgw_0123456789abcdef0123456789abcdef"
)

message = client.messages.create(
    model="claude-3-5-sonnet",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Hello from Anthropic SDK via LLM Gateway"}]
)
print(message.content[0].text)
```

### TypeScript / Node.js

#### OpenAI SDK

```typescript
import OpenAI from "openai";

const openai = new OpenAI({
  baseURL: "http://localhost:8787/v1",
  apiKey: "lgw_0123456789abcdef0123456789abcdef",
});

async function main() {
  const completion = await openai.chat.completions.create({
    messages: [{ role: "user", content: "Say Hello in TypeScript!" }],
    model: "gpt-4o",
  });
  console.log(completion.choices[0].message.content);
}

main();
```

#### Anthropic SDK

```typescript
import Anthropic from "@anthropic-ai/sdk";

const anthropic = new Anthropic({
  baseURL: "http://localhost:8787",
  apiKey: "lgw_0123456789abcdef0123456789abcdef",
});

async function main() {
  const message = await anthropic.messages.create({
    max_tokens: 1024,
    messages: [{ role: "user", content: "Hello from TypeScript Anthropic SDK" }],
    model: "claude-3-5-sonnet",
  });
  console.log(message.content[0]);
}

main();
```

### Raw HTTP / cURL

```bash
# Chat Completions
curl -X POST http://localhost:8787/v1/chat/completions \
  -H "Authorization: Bearer lgw_0123456789abcdef0123456789abcdef" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4o",
    "messages": [{"role": "user", "content": "Ping from cURL"}]
  }'

# Anthropic Messages
curl -X POST http://localhost:8787/v1/messages \
  -H "Authorization: Bearer lgw_0123456789abcdef0123456789abcdef" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-3-5-sonnet",
    "max_tokens": 100,
    "messages": [{"role": "user", "content": "Ping Anthropic from cURL"}]
  }'
```

---

## Key Design Decisions

1. **Database-Per-Service Isolation**:
   - Each microservice (`budget`, `control`, `usage`) connects only to its dedicated schema or database file.
   - Eliminates hidden database couplings and guarantees independent deployability.
2. **Two-Phase Atomic Budget Reservation (`Reserve` -> `Settle`)**:
   - Before executing an upstream LLM call, Gateway reserves estimated worst-case cost via Budget gRPC.
   - If limits (daily/monthly/project) or rate limits are exceeded, Gateway immediately rejects with HTTP 402 or HTTP 429 without hitting providers.
   - Upon provider completion, Gateway settles the exact actual cost in micros. Unused reservation amounts are automatically refunded.
   - If Gateway terminates abruptly, a background sweeper expires pending reservations after TTL.
3. **Fail-Closed Budget Policy (AC-31)**:
   - When the Budget service is unavailable or times out (>200ms), Gateway fails closed (HTTP 503 `budget_unavailable`).
   - Prevents unbudgeted runaway provider costs during infrastructure outages.
4. **Strict Integer Currency (Micros)**:
   - Floating-point arithmetic is strictly prohibited for monetary computations.
   - All balances, costs, token rates, and prices are stored and manipulated as `int64` micro-units ($1.00 USD = 1,000,000 micros).
5. **Asynchronous Ledger via NATS JetStream**:
   - Audit trail and usage events are published to JetStream with at-least-once delivery semantics.
   - If the `usage` service is temporarily down, messages accumulate safely in JetStream streams and are processed immediately upon recovery without event loss.
6. **W3C Distributed Tracing Across Sync & Async Boundaries**:
   - Injected W3C `traceparent` headers are propagated from Edge -> Gateway -> Upstream -> JetStream -> Usage Consumer, providing end-to-end trace visibility in Jaeger.

---

## Observability & Distributed Tracing

A single unified trace spans the full request lifecycle across HTTP, gRPC, and asynchronous NATS events:

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Edge as edge (:8787)
    participant Gateway as gateway (:8788)
    participant Budget as budget (:8792, gRPC :9101)
    participant Upstream as Provider (OpenAI / Anthropic)
    participant NATS as NATS JetStream
    participant Usage as usage (:8791)

    Client->>Edge: POST /v1/chat/completions (traceparent: 00-traceid-...)
    Note over Edge: span: edge.proxy
    Edge->>Gateway: Forward with X-Request-Id & traceparent
    Note over Gateway: span: gateway.request
    Gateway->>Budget: gRPC Reserve(key_id, estimated_micros)
    Note over Budget: span: budget.reserve
    Budget-->>Gateway: OK (reservation_id)
    Gateway->>Upstream: POST /v1/chat/completions
    Note over Upstream: span: upstream.call
    Upstream-->>Gateway: 200 OK (usage tokens)
    Gateway->>Budget: gRPC Settle(reservation_id, actual_micros)
    Note over Budget: span: budget.settle
    Budget-->>Gateway: OK
    Gateway->>NATS: PublishUsageEvent(event) [traceparent injected]
    Gateway-->>Edge: 200 OK
    Edge-->>Client: 200 OK
    NATS-->>Usage: Async Consumer: events.v1.llm_call
    Note over Usage: span: usage.consume_event
    Usage->>Usage: Persist to Ledger & Aggregate
```

---

## Performance & Overhead Benchmark

Overhead is measured as total gateway latency added on top of a zero-latency mock provider: `Edge + Gateway + Budget (Reserve + Settle)`.

Measured on Windows 11 / AMD Ryzen (1,000 sequential requests):

| Metric | Target Specification (AC-39 / AC-1F-11) | Measured Benchmark |
| :--- | :--- | :--- |
| **p50 Latency** | $\le 5.0\text{ ms}$ | **$2.07\text{ ms}$** |
| **p90 Latency** | — | **$6.03\text{ ms}$** |
| **p99 Latency** | $\le 20.0\text{ ms}$ | **$10.50\text{ ms}$** |
| **p99.9 Latency**| — | **$18.00\text{ ms}$** |

To run the automated benchmark locally:
```bash
go test -v ./test/benchmark/...
```

---

## Mechanical Gates & Validation

All code adheres to strict automated quality gates (coverage $\ge 80\%$, zero linter suppressions, cognitive complexity $\le 15$):

```powershell
# Format code
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\format.ps1

# Static analysis, linting, and proto verification
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\lint.ps1

# Unit, integration, and E2E test coverage (threshold >= 80%)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\coverage.ps1

# Static binary build
go build ./cmd/...

# Live runtime health check verification
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\runtime.ps1
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
