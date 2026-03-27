package com.nishanthr.pipeline.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nishanthr.pipeline.model.FailedMessage;
import com.nishanthr.pipeline.model.OrderEvent;
import com.nishanthr.pipeline.persistence.FailedMessageRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    private static final long RETRY_INTERVAL_MS = 2_000L;
    private static final long MAX_ATTEMPTS = 3L;

    private final FailedMessageRepository failedMessageRepository;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.order-events}")
    private String orderEventsTopic;

    public KafkaConsumerConfig(FailedMessageRepository failedMessageRepository,
                                ObjectMapper objectMapper) {
        this.failedMessageRepository = failedMessageRepository;
        this.objectMapper = objectMapper;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        FixedBackOff backOff = new FixedBackOff(RETRY_INTERVAL_MS, MAX_ATTEMPTS);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                this::handlePoisonPill,
                backOff
        );

        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                NullPointerException.class
        );

        return errorHandler;
    }

    private void handlePoisonPill(ConsumerRecord<?, ?> record,
                                   Exception exception,
                                   MessageListenerContainer container) {
        log.error("Message failed after {} retries. Topic={}, Partition={}, Offset={}, Key={}",
                MAX_ATTEMPTS, record.topic(), record.partition(),
                record.offset(), record.key(), exception);

        try {
            OrderEvent event = objectMapper.convertValue(record.value(), OrderEvent.class);

            if (failedMessageRepository.existsByEventId(event.getEventId())) {
                log.warn("Event {} already exists in failed_messages — skipping duplicate",
                        event.getEventId());
                return;
            }

            String payload = objectMapper.writeValueAsString(event);

            FailedMessage failedMessage = FailedMessage.of(
                    event, payload, exception, (int) MAX_ATTEMPTS,
                    record.topic(), record.partition(), record.offset()
            );

            failedMessageRepository.save(failedMessage);

            log.info("Saved failed message [eventId={}, orderId={}]",
                    event.getEventId(), event.getOrderId());

        } catch (Exception e) {
            log.error("CRITICAL: Could not save failed message to DB. Raw payload: {}",
                    record.value(), e);
        }
    }
}