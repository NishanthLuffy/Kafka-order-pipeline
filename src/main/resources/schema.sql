CREATE TABLE IF NOT EXISTS processed_orders (
                                                processed_id    VARCHAR(36)     PRIMARY KEY,
    order_id        VARCHAR(100)    NOT NULL,
    order_type      VARCHAR(50)     NOT NULL,
    channel         VARCHAR(50)     NOT NULL,
    item_count      INT             NOT NULL DEFAULT 0,
    total_cost      DECIMAL(15, 2)  NOT NULL DEFAULT 0.00,
    processed_at    TIMESTAMP       NOT NULL,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
    );

CREATE INDEX IF NOT EXISTS idx_processed_orders_order_id
    ON processed_orders(order_id);

CREATE INDEX IF NOT EXISTS idx_processed_orders_order_type
    ON processed_orders(order_type, channel);

CREATE INDEX IF NOT EXISTS idx_processed_orders_processed_at
    ON processed_orders(processed_at DESC);

CREATE TABLE IF NOT EXISTS failed_messages (
                                               id                  BIGSERIAL       PRIMARY KEY,
                                               event_id            VARCHAR(36)     NOT NULL UNIQUE,
    order_id            VARCHAR(100),
    order_type          VARCHAR(50),
    channel             VARCHAR(50),
    payload             TEXT            NOT NULL,
    error_message       TEXT,
    error_class         VARCHAR(255),
    retry_count         INT             NOT NULL DEFAULT 0,
    kafka_topic         VARCHAR(255),
    kafka_partition     INT,
    kafka_offset        BIGINT,
    status              VARCHAR(20)     NOT NULL DEFAULT 'FAILED',
    failed_at           TIMESTAMP       NOT NULL,
    last_redrive_at     TIMESTAMP,
    redrive_count       INT             NOT NULL DEFAULT 0,
    resolution_notes    TEXT,
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW()
    );

CREATE INDEX IF NOT EXISTS idx_failed_messages_status
    ON failed_messages(status);

CREATE INDEX IF NOT EXISTS idx_failed_messages_order_id
    ON failed_messages(order_id);

CREATE INDEX IF NOT EXISTS idx_failed_messages_failed_at
    ON failed_messages(failed_at DESC);
```

---

**Folder structure reminder — where each file goes:**
```
config/         → KafkaConsumerConfig.java
model/          → FailedMessage.java
persistence/    → FailedMessageRepository.java
consumer/       → RedriveService.java
producer/       → FailedMessageController.java
resources/      → schema.sql (replace existing)