package com.nishanthr.pipeline.producer;

import com.nishanthr.pipeline.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * REST controller that accepts order requests and publishes
 * them to Kafka as OrderEvent messages.
 *
 * Uses orderId as the Kafka message key to ensure all events
 * for the same order land on the same partition (ordering guarantee).
 */
@RestController
@RequestMapping("/api/orders")
public class OrderProducerController {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerController.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${kafka.topics.order-events}")
    private String orderEventsTopic;

    public OrderProducerController(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> publishOrder(@RequestBody OrderEvent event) {
        log.info("Publishing order event [orderId={}, type={}, channel={}]",
                event.getOrderId(), event.getOrderType(), event.getChannel());

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(orderEventsTopic, event.getOrderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish order [orderId={}]: {}", event.getOrderId(), ex.getMessage());
            } else {
                log.info("Published order [orderId={}, partition={}, offset={}]",
                        event.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return ResponseEntity.accepted().body(Map.of(
                "status", "PUBLISHED",
                "eventId", event.getEventId(),
                "orderId", event.getOrderId(),
                "timestamp", Instant.now().toString()
        ));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }
}
