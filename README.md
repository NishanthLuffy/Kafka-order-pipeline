# kafka-order-pipeline

An event-driven order processing pipeline built with Apache Kafka and Spring Boot, demonstrating production-grade patterns used in high-throughput systems processing 100,000+ orders daily.

---

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────────────────┐     ┌────────────────┐
│  Order Producer │────▶│ Kafka Topic │────▶│  Event Processor Consumer │────▶│   PostgreSQL   │
│  (REST API)     │     │order-events │     │  (Strategy Pattern)       │     │  (Persistence) │
└─────────────────┘     └─────────────┘     └──────────────────────────┘     └────────────────┘
                                                        │
                              ┌─────────────────────────┼─────────────────────────┐
                              ▼                         ▼                         ▼
                   ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
                   │  STAPLESTOCK     │    │      DSD         │    │   CROSSDOCK      │
                   │  Strategy        │    │   Strategy       │    │   Strategy       │
                   └──────────────────┘    └──────────────────┘    └──────────────────┘
```

### How it works

1. A REST endpoint accepts an order request and publishes it to a Kafka topic
2. The consumer picks up the event and resolves the correct processing strategy using a composite key (`orderType/channel`)
3. The strategy performs enrichment (item details, location, supplier) and validation
4. The validated, enriched order is persisted to PostgreSQL
5. Failed events are routed to a dead-letter topic for retry

---

## Design decisions

### Why the Strategy pattern?

Each order channel (Staplestock, DSD, Crossdock) has distinct enrichment and validation logic. A naive approach uses a giant `if/else` or `switch` block — this breaks open/closed principle and requires touching core logic every time a new channel is added.

Instead, each strategy is a Spring-managed bean that auto-registers into a registry via a composite key. Adding a new channel means adding one new class and zero modifications to existing code. At Walmart, this pattern cut new channel onboarding effort by ~80%.

### Why Kafka over a REST call to downstream?

Decoupling. The producer doesn't need to know if the consumer is up, slow, or being deployed. Events are durable — if the consumer crashes mid-batch, it resumes from its last committed offset. This matters at 100k+ events/day where losing even a fraction of events has real business impact.

### Why chunk-based processing?

Large batch orders (10k+ lines) are broken into smaller chunks before enrichment. This limits memory pressure, allows parallel processing of chunks, and means a failure at line 8,000 doesn't require reprocessing lines 1–7,999.

---

## Project structure

```
kafka-order-pipeline/
├── src/main/java/com/nishanthr/pipeline/
│   ├── config/           # Kafka producer/consumer config, topic setup
│   ├── producer/         # REST controller + Kafka producer
│   ├── consumer/         # Kafka listener — entry point for processing
│   ├── strategy/         # Strategy interface + registry
│   │   └── impl/         # Channel-specific strategy implementations
│   ├── model/            # OrderRequest, OrderEvent, ProcessedOrder POJOs
│   ├── enrichment/       # Item, location, supplier enrichment services
│   ├── persistence/      # Spring Data JPA repositories
│   └── util/             # Chunk splitter, deduplication helpers
├── src/main/resources/
│   └── application.yml   # Kafka, DB, app config
├── src/test/             # Unit + integration tests
├── docker-compose.yml    # Kafka + Zookeeper + Postgres — runs locally in one command
└── README.md
```

---

## Getting started

### Prerequisites

- Java 17+
- Docker + Docker Compose
- Maven 3.8+

### Run locally

```bash
# 1. Clone the repo
git clone https://github.com/NishanthLuffy/kafka-order-pipeline.git
cd kafka-order-pipeline

# 2. Start Kafka, Zookeeper, and Postgres
docker-compose up -d

# 3. Build and run the app
mvn spring-boot:run

# 4. Send a test order event
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORD-001",
    "orderType": "STAPLESTOCK",
    "channel": "US",
    "items": [
      { "itemId": "ITEM-123", "quantity": 50, "unitCost": 12.99 }
    ]
  }'

# 5. Check the processed order in Postgres
docker exec -it postgres psql -U pipeline -d orders -c "SELECT * FROM processed_orders;"
```

---

## API

### POST /api/orders
Publishes an order event to Kafka.

**Request body**
```json
{
  "orderId": "string",
  "orderType": "STAPLESTOCK | DSD | CROSSDOCK",
  "channel": "string",
  "items": [
    {
      "itemId": "string",
      "quantity": "number",
      "unitCost": "number"
    }
  ]
}
```

**Response**
```json
{
  "status": "PUBLISHED",
  "eventId": "uuid",
  "timestamp": "ISO-8601"
}
```

---

## Key patterns demonstrated

| Pattern | Where used | Why |
|---------|-----------|-----|
| Strategy + Registry | `EventProcessorRegistry` | Auto-registers channel strategies via Spring — zero factory modifications for new channels |
| Chunk-based processing | `ChunkSplitter` util | Limits memory pressure on large batches, enables parallel processing |
| Dead-letter topic | Kafka consumer error handler | Failed events are not lost — routed for retry or inspection |
| Idempotent consumer | Deduplication util | Prevents double-processing when Kafka redelivers events |
| Outbox pattern | Persistence layer | Ensures DB write and Kafka publish are atomic |

---

## Running tests

```bash
mvn test
```

Integration tests use Testcontainers to spin up real Kafka and Postgres instances — no mocks.

---

## Tech stack

- Java 17
- Spring Boot 3
- Apache Kafka
- Spring Data JPA
- PostgreSQL
- Docker + Docker Compose
- Testcontainers (integration tests)
- Maven

---

## Roadmap

- [ ] Add Grafana + Prometheus monitoring dashboard
- [ ] Add Cassandra as an alternative persistence layer
- [ ] Add GraphQL read API for order status queries
- [ ] Add batch order endpoint (bulk publish)

---

## Author

**Nishanth Reddy Bandaru** — [GitHub](https://github.com/NishanthLuffy) · [LinkedIn](https://linkedin.com/in/YOUR_LINKEDIN_HANDLE)
