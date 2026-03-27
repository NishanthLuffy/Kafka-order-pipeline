package com.nishanthr.pipeline.persistence;

import com.nishanthr.pipeline.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Repository
public class OrderRepository {

    private static final Logger log = LoggerFactory.getLogger(OrderRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Persists a processed order only if it hasn't been processed before.
     * Uses event_id as the idempotency key — safe to call multiple times
     * with the same event (Kafka redelivery, consumer rebalance, redrive).
     *
     * Returns the processedId — either newly created or the existing one.
     */
    public String save(OrderEvent event) {

        // Idempotency check — if this event was already processed, return existing ID
        String existingId = findProcessedIdByEventId(event.getEventId());
        if (existingId != null) {
            log.warn("Duplicate event detected — already processed [eventId={}, processedId={}]. " +
                    "Skipping insert.", event.getEventId(), existingId);
            return existingId;
        }

        String processedId = UUID.randomUUID().toString();
        int itemCount = event.getItems() != null ? event.getItems().size() : 0;
        double totalCost = event.getItems() != null
                ? event.getItems().stream().mapToDouble(i -> i.getTotalCost()).sum()
                : 0.0;

        try {
            jdbcTemplate.update(
                    """
                    INSERT INTO processed_orders
                      (processed_id, event_id, order_id, order_type, channel,
                       item_count, total_cost, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    processedId,
                    event.getEventId(),   // idempotency key stored in DB
                    event.getOrderId(),
                    event.getOrderType(),
                    event.getChannel(),
                    itemCount,
                    totalCost,
                    Timestamp.from(Instant.now())
            );

            log.debug("Persisted order [eventId={}, orderId={}, processedId={}, items={}, totalCost={}]",
                    event.getEventId(), event.getOrderId(), processedId, itemCount, totalCost);

        } catch (org.springframework.dao.DuplicateKeyException e) {
            // Race condition — two instances tried to insert the same event simultaneously
            // The UNIQUE constraint on event_id caught it — safe to ignore
            log.warn("Race condition on insert — event already persisted [eventId={}]. " +
                    "UNIQUE constraint protected us.", event.getEventId());
            return findProcessedIdByEventId(event.getEventId());
        }

        return processedId;
    }

    /**
     * Looks up an existing processed order by event_id.
     * Returns null if not found.
     */
    private String findProcessedIdByEventId(String eventId) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT processed_id FROM processed_orders WHERE event_id = ?",
                    String.class,
                    eventId
            );
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return null;
        }
    }
}