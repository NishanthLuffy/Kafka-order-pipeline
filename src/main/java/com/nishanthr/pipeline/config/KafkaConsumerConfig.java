package com.nishanthr.pipeline.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.nishanthr.pipeline.model.FailedMessage;
import com.nishanthr.pipeline.model.OrderEvent;
import com.nishanthr.pipeline.persistence.FailedMessageRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    private static final long RETRY_INTERVAL_MS = 2_000L;
    private static final long MAX_ATTEMPTS = 3L;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    private final FailedMessageRepository failedMessageRepository;

    public KafkaConsumerConfig(FailedMessageRepository failedMessageRepository) {
        this.failedMessageRepository = failedMessageRepository;
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @Bean
    public ConsumerFactory<String, OrderEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<OrderEvent> deserializer = new JsonDeserializer<>(OrderEvent.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(true);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        FixedBackOff backOff = new FixedBackOff(RETRY_INTERVAL_MS, MAX_ATTEMPTS);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (ConsumerRecord<?, ?> record, Exception exception) -> {
                    log.error("Message failed after {} retries. Topic={}, Partition={}, Offset={}, Key={}",
                            MAX_ATTEMPTS, record.topic(), record.partition(),
                            record.offset(), record.key(), exception);

                    try {
                        ObjectMapper mapper = objectMapper();
                        OrderEvent event = mapper.convertValue(record.value(), OrderEvent.class);

                        if (failedMessageRepository.existsByEventId(event.getEventId())) {
                            log.warn("Event {} already in failed_messages — skipping",
                                    event.getEventId());
                            return;
                        }

                        String payload = mapper.writeValueAsString(event);

                        FailedMessage failedMessage = FailedMessage.of(
                                event, payload, exception, (int) MAX_ATTEMPTS,
                                record.topic(), record.partition(), record.offset()
                        );

                        failedMessageRepository.save(failedMessage);

                        log.info("Saved to failed_messages [eventId={}, orderId={}]",
                                event.getEventId(), event.getOrderId());

                    } catch (Exception e) {
                        log.error("CRITICAL: Could not save to failed_messages. Raw: {}",
                                record.value(), e);
                    }
                },
                backOff
        );

        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                NullPointerException.class
        );

        return errorHandler;
    }
}