package com.nishanthr.pipeline.model;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "failed_messages")
public class FailedMessage {

    public enum Status {
        FAILED, REDRIVING, RESOLVED, DEAD
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "order_id")
    private String orderId;

    @Column(name = "order_type")
    private String orderType;

    @Column(name = "channel")
    private String channel;

    @Column(name = "payload", columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "error_class")
    private String errorClass;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "kafka_topic")
    private String kafkaTopic;

    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private Status status = Status.FAILED;

    @Column(name = "failed_at", nullable = false)
    private Instant failedAt;

    @Column(name = "last_redrive_at")
    private Instant lastRedriveAt;

    @Column(name = "redrive_count", nullable = false)
    private int redriveCount = 0;

    @Column(name = "resolution_notes", columnDefinition = "TEXT")
    private String resolutionNotes;

    public FailedMessage() {}

    public static FailedMessage of(OrderEvent event, String payload,
                                    Throwable ex, int retryCount,
                                    String topic, int partition, long offset) {
        FailedMessage fm = new FailedMessage();
        fm.eventId = event.getEventId();
        fm.orderId = event.getOrderId();
        fm.orderType = event.getOrderType();
        fm.channel = event.getChannel();
        fm.payload = payload;
        fm.errorMessage = ex.getMessage();
        fm.errorClass = ex.getClass().getName();
        fm.retryCount = retryCount;
        fm.kafkaTopic = topic;
        fm.kafkaPartition = partition;
        fm.kafkaOffset = offset;
        fm.status = Status.FAILED;
        fm.failedAt = Instant.now();
        return fm;
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getOrderId() { return orderId; }
    public String getOrderType() { return orderType; }
    public String getChannel() { return channel; }
    public String getPayload() { return payload; }
    public String getErrorMessage() { return errorMessage; }
    public String getErrorClass() { return errorClass; }
    public int getRetryCount() { return retryCount; }
    public String getKafkaTopic() { return kafkaTopic; }
    public Integer getKafkaPartition() { return kafkaPartition; }
    public Long getKafkaOffset() { return kafkaOffset; }
    public Status getStatus() { return status; }
    public Instant getFailedAt() { return failedAt; }
    public Instant getLastRedriveAt() { return lastRedriveAt; }
    public int getRedriveCount() { return redriveCount; }
    public String getResolutionNotes() { return resolutionNotes; }

    public void setStatus(Status status) { this.status = status; }
    public void setLastRedriveAt(Instant t) { this.lastRedriveAt = t; }
    public void setRedriveCount(int c) { this.redriveCount = c; }
    public void setResolutionNotes(String n) { this.resolutionNotes = n; }
}