package com.nishanthr.pipeline.consumer;

import com.nishanthr.pipeline.model.OrderEvent;
import com.nishanthr.pipeline.strategy.EventProcessorRegistry;
import com.nishanthr.pipeline.strategy.OrderProcessingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer — entry point for all incoming order events.
 *
 * Resolves the correct processing strategy from the registry
 * using the event's composite key, then delegates enrichment,
 * validation, and processing to that strategy.
 */
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final EventProcessorRegistry registry;

    public OrderEventConsumer(EventProcessorRegistry registry) {
        this.registry = registry;
    }

    @KafkaListener(
            topics = "${kafka.topics.order-events}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) throws Exception {
        log.info("Received order event [orderId={}, type={}, channel={}, partition={}, offset={}]",
                event.getOrderId(), event.getOrderType(), event.getChannel(), partition, offset);

        String strategyKey = event.getStrategyKey();

        if (!registry.supports(strategyKey)) {
            log.error("No strategy for key '{}' — routing to DLQ", strategyKey);
            // In production: publish to DLQ topic
            return;
        }

        try {
            OrderProcessingStrategy strategy = registry.resolve(strategyKey);

            // Pipeline: enrich → validate → process
            OrderEvent enriched = strategy.enrich(event);
            strategy.validate(enriched);
            String processedId = strategy.process(enriched);

            log.info("Successfully processed order [orderId={}, processedId={}]",
                    event.getOrderId(), processedId);

        } catch (IllegalArgumentException e) {
            log.error("Validation failed for order [orderId={}]: {}", event.getOrderId(), e.getMessage());
            // In production: publish to DLQ with error metadata
        } catch (Exception e) {
            log.error("Unexpected error processing order [orderId={}]", event.getOrderId(), e);
            throw e; // Re-throw so Kafka retries via retry policy
        }
    }
}
