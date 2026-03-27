package com.nishanthr.pipeline.persistence;

import com.nishanthr.pipeline.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

/**
 * Repository for persisting processed orders to PostgreSQL.
 *
 * Uses JdbcTemplate directly for performance on high-volume
 * batch inserts — JPA overhead adds up at 100k+ orders/day.
 */
@Repository
public class OrderRepository {

    private static final Logger log = LoggerFactory.getLogger(OrderRepository.class);

    private final JdbcTemplate jdbcTemplate;

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Persists a processed order event and returns the generated processed ID.
     */
    public String save(OrderEvent event) {
        String processedId = UUID.randomUUID().toString();
        int itemCount = event.getItems() != null ? event.getItems().size() : 0;
        double totalCost = event.getItems() != null
                ? event.getItems().stream().mapToDouble(i -> i.getTotalCost()).sum()
                : 0.0;

        jdbcTemplate.update(
                """
                INSERT INTO processed_orders
                  (processed_id, order_id, order_type, channel, item_count, total_cost, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                processedId,
                event.getOrderId(),
                event.getOrderType(),
                event.getChannel(),
                itemCount,
                totalCost,
                Timestamp.from(Instant.now())
        );

        log.debug("Persisted order [orderId={}, processedId={}, items={}, totalCost={}]",
                event.getOrderId(), processedId, itemCount, totalCost);

        return processedId;
    }
}
