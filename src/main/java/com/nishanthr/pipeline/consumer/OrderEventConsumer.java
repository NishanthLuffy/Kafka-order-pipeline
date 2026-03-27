package com.nishanthr.pipeline.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nishanthr.pipeline.model.FailedMessage;
import com.nishanthr.pipeline.model.OrderEvent;
import com.nishanthr.pipeline.persistence.FailedMessageRepository;
import com.nishanthr.pipeline.strategy.EventProcessorRegistry;
import com.nishanthr.pipeline.strategy.OrderProcessingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final EventProcessorRegistry registry;
    private final FailedMessageRepository failedMessageRepository;
    private final ObjectMapper objectMapper;

    public OrderEventConsumer(EventProcessorRegistry registry,
                              FailedMessageRepository failedMessageRepository,
                              ObjectMapper objectMapper) {
        this.registry = registry;
        this.failedMessageRepository = failedMessageRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${kafka.topics.order-events}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic
    ) {
        log.info("Received order event [orderId={}, type={}, channel={}, partition={}, offset={}]",
                event.getOrderId(), event.getOrderType(), event.getChannel(), partition, offset);

        String strategyKey = event.getStrategyKey();

        // No strategy found — save to failed_messages immediately, no point retrying
        if (!registry.supports(strategyKey)) {
            log.error("No strategy for key '{}' — saving to failed_messages", strategyKey);
            saveToFailedMessages(event, topic, partition, offset,
                    new IllegalArgumentException("No strategy registered for key: " + strategyKey));
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
            // Validation failure — not retryable, save to failed_messages directly
            log.error("Validation failed for order [orderId={}]: {}", event.getOrderId(), e.getMessage());
            saveToFailedMessages(event, topic, partition, offset, e);

        } catch (Exception e) {
            // Transient failure — re-throw so KafkaConsumerConfig retries 3 times
            // After 3 retries the recoverer in KafkaConsumerConfig saves to failed_messages
            log.error("Transient error processing order [orderId={}] — triggering retry",
                    event.getOrderId(), e);
            throw e;
        }
    }

    private void saveToFailedMessages(OrderEvent event, String topic,
                                      int partition, long offset, Exception e) {
        try {
            if (failedMessageRepository.existsByEventId(event.getEventId())) {
                log.warn("Event {} already in failed_messages — skipping", event.getEventId());
                return;
            }
            String payload = objectMapper.writeValueAsString(event);
            FailedMessage fm = FailedMessage.of(event, payload, e, 0, topic, partition, offset);
            failedMessageRepository.save(fm);
            log.info("Saved to failed_messages [orderId={}, reason={}]",
                    event.getOrderId(), e.getMessage());
        } catch (Exception ex) {
            log.error("CRITICAL: Could not save to failed_messages [orderId={}]",
                    event.getOrderId(), ex);
        }
    }
}